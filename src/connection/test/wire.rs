//! Tests of the connection against a hand-driven peer, frame by frame.

use std::{
    fmt::Debug,
    future::Future,
    io,
    num::{NonZeroU16, NonZeroU32},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, Sink, SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, ReadHalf, WriteHalf},
    time::Instant,
};

use crate::{
    StreamId,
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress, active::CREDIT_BURST},
    stream::RammuxDuplex,
    wire::{
        codec::{
            RammuxCodec,
            decoder::{DecodedFrame, InboundFrame},
        },
        header::{ControlFlags, Header, MAX_HEADER_LEN, Pong, StreamHeader},
    },
};

/// A rammux connection and a peer that reads its frames and writes raw bytes to it.
pub(super) struct Rig {
    pub(super) conn: RammuxConnection<DuplexStream>,
    pub(super) peer_in: RammuxCodec<ReadHalf<DuplexStream>>,
    pub(super) peer_out: WriteHalf<DuplexStream>,
}

fn config() -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(256).unwrap();
    config.inbound_frame_timeout = Duration::from_secs(3600);
    config
}

pub(super) fn rig(role: RammuxRole, config: RammuxConfig) -> Rig {
    let (io_1, io_2) = tokio::io::duplex(1 << 20);
    let (read, write) = tokio::io::split(io_2);
    Rig {
        conn: RammuxConnection::new(role, io_1, config.clone()).unwrap(),
        peer_in: RammuxCodec::new(read, config.frame_limit, Duration::from_secs(3600)),
        peer_out: write,
    }
}

pub(super) fn raw(header: Header, payload: &[u8]) -> Vec<u8> {
    let mut buf = [0; MAX_HEADER_LEN];
    let len = header.encode(&mut buf);
    let mut out = buf[..len].to_vec();
    out.extend_from_slice(payload);
    out
}

fn stream_header(id: u16, flags: ControlFlags, data: usize) -> Option<StreamHeader> {
    Some(StreamHeader {
        id: StreamId::from_be_bytes(id.to_be_bytes()),
        flags,
        credit: None,
        data_len: NonZeroU16::new(u16::try_from(data).unwrap()),
    })
}

fn syn() -> ControlFlags {
    ControlFlags {
        syn: true,
        ..Default::default()
    }
}

impl Rig {
    /// Lets the connection process everything the peer has written.
    fn settle(&mut self) {
        while let Some(result) = self.conn.progress().now_or_never() {
            result.unwrap();
        }
    }

    async fn write(&mut self, bytes: Vec<u8>) {
        self.peer_out.write_all(&bytes).await.unwrap();
        self.settle();
    }

    /// Reads the frames the connection has written so far.
    fn frames(&mut self) -> Vec<InboundFrame> {
        self.settle();
        let mut frames = Vec::new();
        while let Some(Some(frame)) = self.peer_in.next().now_or_never() {
            match frame.unwrap() {
                DecodedFrame::Frame(frame) => frames.push(frame),
                DecodedFrame::Terminate => panic!("unexpected TERM"),
            }
        }
        frames
    }

    fn pongs(&mut self) -> Vec<Pong> {
        self.frames()
            .into_iter()
            .filter_map(|frame| frame.header.pong)
            .collect()
    }
}

/// Drives the connection until the future completes.
async fn driving<T>(conn: &mut RammuxConnection<DuplexStream>, fut: impl Future<Output = T>) -> T {
    tokio::select! {
        out = fut => out,
        result = async { loop { conn.progress().await.unwrap(); } } => result,
    }
}

#[tokio::test(start_paused = true)]
async fn pings_read_one_by_one_are_each_answered_in_order() {
    let mut rig = rig(RammuxRole::Server, config());
    // Skip the PING the connection sends on its own.
    rig.frames();

    for t_ping in [10, 11, 10, 5] {
        let header = Header {
            ping: Some(t_ping),
            ..Default::default()
        };
        rig.write(raw(header, b"")).await;
    }
    let pongs = rig.pongs();
    assert_eq!(
        pongs.iter().map(|pong| pong.t_echo).collect::<Vec<_>>(),
        [10, 11, 10, 5]
    );
    assert!(rig.pongs().is_empty(), "no PONG is repeated");
}

/// A PONG that carries the given echo.
fn pong_of(t_echo: u32) -> Vec<u8> {
    raw(
        Header {
            pong: Some(Pong { t_echo }),
            ..Default::default()
        },
        b"",
    )
}

impl Rig {
    /// Makes the connection send a PING and returns its `t_ping`.
    async fn send_ping(&mut self, after: Duration) -> u32 {
        tokio::time::advance(after).await;
        self.frames()
            .into_iter()
            .filter_map(|frame| frame.header.ping)
            .next_back()
            .expect("a PING is due")
    }
}

#[tokio::test(start_paused = true)]
async fn pong_gives_the_round_trip_time() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    assert_eq!(rig.conn.stats().srtt, None);

    let t_ping = rig.send_ping(Duration::from_secs(5)).await;
    tokio::time::advance(Duration::from_millis(5)).await;
    rig.write(pong_of(t_ping)).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));
}

#[tokio::test(start_paused = true)]
async fn a_pong_that_waited_longer_than_the_frame_timeout_is_a_sample() {
    // The timeout limits the gaps between frames, not the latency: frames keep arriving while the PONG
    // waits in a queue behind other data.
    let mut config = config();
    config.inbound_frame_timeout = Duration::from_secs(10);
    config.ping_interval = Duration::from_secs(5);
    let mut rig = rig(RammuxRole::Server, config);
    rig.frames();
    let t_ping = rig.send_ping(Duration::from_secs(5)).await;
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(4)).await;
        rig.write(raw(
            Header {
                ping: Some(1),
                ..Default::default()
            },
            b"",
        ))
        .await;
        rig.frames();
    }
    rig.write(pong_of(t_ping)).await;
    assert!(rig.conn.stats().srtt.unwrap() >= Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn samples_stay_right_across_the_wrap_of_the_clock() {
    let mut config = config();
    config.inbound_frame_timeout = Duration::from_secs(10);
    config.ping_interval = Duration::from_secs(5);
    let mut rig = rig(RammuxRole::Server, config);
    rig.frames();
    // The clock wraps after 2^32 us, about 71.6 minutes. Every PING is answered after 5 ms.
    for _ in 0..(75 * 60 / 5) {
        let t_ping = rig.send_ping(Duration::from_secs(5)).await;
        tokio::time::advance(Duration::from_millis(5)).await;
        rig.write(pong_of(t_ping)).await;
        assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));
    }
}

#[tokio::test(start_paused = true)]
async fn an_endless_frame_timeout_works() {
    let mut config = config();
    config.inbound_frame_timeout = Duration::MAX;
    let mut rig = rig(RammuxRole::Server, config);
    rig.frames();
    rig.write(raw(
        Header {
            ping: Some(1),
            ..Default::default()
        },
        b"",
    ))
    .await;
    assert_eq!(rig.pongs(), [Pong { t_echo: 1 }]);
}

#[tokio::test(start_paused = true)]
async fn an_endless_ping_interval_works() {
    let mut config = config();
    config.ping_interval = Duration::from_secs(u64::MAX);
    config.inbound_frame_timeout = Duration::MAX;
    let mut rig = rig(RammuxRole::Server, config);
    // The first PING is due right away, the next one never.
    let pings = rig
        .frames()
        .iter()
        .filter(|f| f.header.ping.is_some())
        .count();
    assert_eq!(pings, 1);
    // Less than the frame timeout of the peer in the rig.
    tokio::time::advance(Duration::from_secs(1800)).await;
    let pings = rig
        .frames()
        .iter()
        .filter(|f| f.header.ping.is_some())
        .count();
    assert_eq!(pings, 0);
}

#[tokio::test(start_paused = true)]
async fn the_reserved_bit_is_a_protocol_violation() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    // PING with the reserved bit 0x0100 set.
    rig.peer_out
        .write_all(&[0x01, 0x20, 0, 0, 0, 1])
        .await
        .unwrap();
    let error = rig.conn.progress().await.err().unwrap();
    assert!(error.is_protocol_violation());
}

/// A frame the connection wrote, as the raw peer saw it.
struct Seen {
    at: Instant,
    t_ping: Option<u32>,
    flow: bool,
}

/// Keeps a stream writing 100 bytes every millisecond for `millis`, while the peer returns the credit
/// of everything it read. Returns the frames the peer saw.
async fn write_for(
    rig: &mut Rig,
    sink: &mut (impl Sink<Bytes, Error: Debug> + Unpin),
    millis: u64,
) -> Vec<Seen> {
    let mut seen = Vec::new();
    for _ in 0..millis {
        let chunk = Bytes::from(vec![7_u8; 100]);
        driving(&mut rig.conn, async {
            sink.send(chunk).await.unwrap();
            sink.flush().await.unwrap();
        })
        .await;
        let mut read = 0;
        for frame in rig.frames() {
            let stream = frame.header.stream.as_ref();
            read += frame.data.as_ref().len();
            seen.push(Seen {
                at: Instant::now(),
                t_ping: frame.header.ping,
                flow: stream.is_some_and(|s| s.credit.is_some() || s.data_len.is_some()),
            });
        }
        if read > 0 {
            let credit = u32::try_from(read).unwrap();
            rig.write(raw(credit_header(0, ControlFlags::default(), credit), b""))
                .await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;
    }
    seen
}

/// The frames with a `PING`.
fn pings_of(seen: &[Seen]) -> Vec<&Seen> {
    seen.iter().filter(|frame| frame.t_ping.is_some()).collect()
}

#[tokio::test(start_paused = true)]
async fn pings_ride_on_flow_frames_and_back_off() {
    let mut rig = rig(RammuxRole::Client, config());
    let stream = rig.conn.try_start_outbound().unwrap().unwrap();
    let (mut sink, _stream) = stream.into_split();
    let start = Instant::now();

    // The peer never answers a PING: the gap doubles with every one.
    let mut seen = write_for(&mut rig, &mut sink, 100).await;
    // The PONG comes 40 ms after the last PING.
    let last = *pings_of(&seen).last().unwrap();
    let (t_ping, sent_at) = (last.t_ping.unwrap(), last.at);
    let elapsed = u64::try_from((Instant::now() - sent_at).as_millis()).unwrap();
    seen.extend(write_for(&mut rig, &mut sink, 40_u64.saturating_sub(elapsed)).await);

    let pings = pings_of(&seen);
    assert_eq!(pings[0].at, start);
    for ping in &pings[1..] {
        assert!(ping.flow, "a PING after the first rides on DATA or WINDOW");
    }
    let mut min_gap = Duration::from_millis(10);
    for pair in pings.windows(2) {
        assert!(pair[1].at - pair[0].at >= min_gap, "{min_gap:?}");
        min_gap *= 2;
    }
    assert!(pings.len() >= 4, "{}", pings.len());
    assert_eq!(pings.last().unwrap().t_ping, Some(t_ping));

    // A PONG makes the round trip time 40 ms.
    rig.write(pong_of(t_ping)).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(40)));

    // From now on, the gap is an eighth of it, and the stream writes every millisecond: a PING every 5 ms.
    let seen = write_for(&mut rig, &mut sink, 100).await;
    let pings = pings_of(&seen);
    assert_eq!(pings.len(), 20);
    for ping in &pings {
        assert!(ping.flow);
    }
    for pair in pings.windows(2) {
        assert_eq!(pair[1].at - pair[0].at, Duration::from_millis(5));
    }
}

fn credit_header(id: u16, flags: ControlFlags, credit: u32) -> Header {
    Header {
        stream: Some(StreamHeader {
            id: StreamId::from_be_bytes(id.to_be_bytes()),
            flags,
            credit: NonZeroU32::new(credit),
            data_len: None,
        }),
        ..Default::default()
    }
}

fn fin_read() -> ControlFlags {
    ControlFlags {
        fin_read: true,
        ..syn()
    }
}

fn fin_write() -> ControlFlags {
    ControlFlags {
        fin_write: true,
        ..Default::default()
    }
}

/// Feeds the frames to a server and expects it to fail with a protocol violation at the end of them.
async fn violation(config: RammuxConfig, frames: Vec<Vec<u8>>) {
    let mut rig = rig(RammuxRole::Server, config);
    rig.frames();
    let mut streams = Vec::new();
    for frame in &frames[..frames.len() - 1] {
        rig.peer_out.write_all(frame).await.unwrap();
        loop {
            match rig.conn.progress().now_or_never() {
                Some(Ok(RammuxProgress::Inbound(stream))) => streams.push(stream),
                Some(Ok(_)) => {},
                Some(Err(error)) => panic!("failed too early: {error}"),
                None => break,
            }
        }
    }
    rig.peer_out
        .write_all(frames.last().unwrap())
        .await
        .unwrap();
    let error = loop {
        match rig.conn.progress().now_or_never() {
            Some(Ok(RammuxProgress::Inbound(stream))) => streams.push(stream),
            Some(Ok(_)) => {},
            Some(Err(error)) => break error,
            None => panic!("the peer got away with it"),
        }
    };
    assert!(error.is_protocol_violation(), "{error}");
}

fn data(id: u16, flags: ControlFlags, payload: &[u8]) -> Vec<u8> {
    let header = Header {
        stream: stream_header(id, flags, payload.len()),
        ..Default::default()
    };
    raw(header, payload)
}

fn control(id: u16, flags: ControlFlags) -> Vec<u8> {
    raw(credit_header(id, flags, 0), b"")
}

#[tokio::test(start_paused = true)]
async fn stream_lifecycle_violations() {
    let none = ControlFlags::default();
    // A stream starts without SYN.
    violation(config(), vec![data(0, none, b"x")]).await;
    // A stream starts twice.
    violation(config(), vec![data(0, syn(), b"x"), data(0, syn(), b"x")]).await;
    // A stream starts with an ID of the wrong side.
    violation(config(), vec![data(1, syn(), b"x")]).await;
    // Too many streams.
    let mut limited = config();
    limited.max_inbound_streams = 1;
    violation(limited, vec![data(0, syn(), b"x"), data(2, syn(), b"x")]).await;
    // A frame for a stream that does not exist.
    violation(config(), vec![data(2, none, b"x")]).await;
    // DATA after FIN_WRITE, a second FIN_WRITE, a FIN_WRITE | ABORT after FIN_WRITE.
    let fw = ControlFlags {
        syn: true,
        ..fin_write()
    };
    violation(config(), vec![data(0, fw, b"x"), data(0, none, b"x")]).await;
    violation(config(), vec![data(0, fw, b"x"), control(0, fin_write())]).await;
    let abort = ControlFlags {
        abort: true,
        ..fin_write()
    };
    violation(config(), vec![data(0, fw, b"x"), control(0, abort)]).await;
    // A second FIN_READ, WINDOW after FIN_READ.
    let fr = ControlFlags {
        fin_read: true,
        ..none
    };
    violation(config(), vec![control(0, fin_read()), control(0, fr)]).await;
    let window = raw(credit_header(0, none, 1), b"");
    violation(config(), vec![control(0, fin_read()), window]).await;
}

/// `FIN_WRITE` with `RESET` is a `FIN_WRITE`: it comes once, and no data follows it.
#[tokio::test(start_paused = true)]
async fn reset_violations() {
    let none = ControlFlags::default();
    let fw = ControlFlags {
        syn: true,
        ..fin_write()
    };
    let reset = ControlFlags {
        reset: true,
        ..fin_write()
    };
    let reset_syn = ControlFlags { syn: true, ..reset };
    violation(config(), vec![data(0, fw, b"x"), control(0, reset)]).await;
    violation(
        config(),
        vec![data(0, reset_syn, b"x"), control(0, fin_write())],
    )
    .await;
    violation(
        config(),
        vec![data(0, reset_syn, b"x"), data(0, none, b"x")],
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn flow_control_violations() {
    let none = ControlFlags::default();
    // More credit than there is data in flight.
    violation(config(), vec![raw(credit_header(0, syn(), 1), b"")]).await;

    // The base window is 256, the pool 256 bytes.
    let mut tight = config();
    tight.local_recv_window = NonZeroU32::new(256).unwrap();
    tight.local_recv_pool = 256;
    // Within the base window and the pool, nothing is wrong.
    let mut rig_ok = rig(RammuxRole::Server, tight.clone());
    rig_ok.frames();
    rig_ok.write(data(0, syn(), &[1; 256])).await;
    rig_ok.write(data(0, none, &[1; 256])).await;
    drop(rig_ok);
    // One byte more is a violation.
    violation(
        tight.clone(),
        vec![
            data(0, syn(), &[1; 256]),
            data(0, none, &[1; 256]),
            data(0, none, &[1; 1]),
        ],
    )
    .await;
    // The pool is shared: two streams cannot both have 200 bytes of it.
    violation(
        tight,
        vec![
            data(0, syn(), &[1; 256]),
            data(0, none, &[1; 200]),
            data(2, syn(), &[1; 256]),
            data(2, none, &[1; 57]),
        ],
    )
    .await;
}

/// A transport that reads from one pipe and writes to another, which can have different sizes.
struct Pipes {
    read: DuplexStream,
    write: DuplexStream,
}

impl AsyncRead for Pipes {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl AsyncWrite for Pipes {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

/// Lets the connection do everything it can do now. Returns the streams that the peer started.
fn settle(conn: &mut RammuxConnection<Pipes>) -> Vec<RammuxDuplex> {
    let mut started = Vec::new();
    // Unconstrained: tokio's budget would make a long run of ready reads look like Pending.
    while let Some(result) = tokio::task::unconstrained(conn.progress()).now_or_never() {
        if let RammuxProgress::Inbound(stream) = result.unwrap() {
            started.push(stream);
        }
    }
    started
}

#[tokio::test(start_paused = true)]
async fn a_flood_of_pings_is_read_without_a_stall_and_answered_with_the_latest() {
    // A peer that sends only PINGs and does not read for a while. The answers do not fit the pipe
    // back, but the connection must keep reading, and hold at most one answer.
    let (read, mut peer_out) = tokio::io::duplex(1 << 20);
    let (write, mut peer_in) = tokio::io::duplex(1024);
    let mut conn =
        RammuxConnection::new(RammuxRole::Server, Pipes { read, write }, config()).unwrap();
    let owed = |conn: &RammuxConnection<Pipes>| conn.state.active().unwrap().owed_pong();
    let ping = |t_ping| {
        raw(
            Header {
                ping: Some(t_ping),
                ..Default::default()
            },
            b"",
        )
    };
    let sent = 10_000;
    for t_ping in 0..sent {
        peer_out.write_all(&ping(t_ping)).await.unwrap();
    }
    settle(&mut conn);
    // Every frame was read despite the full pipe back: the last PING is the one owed, the only slot.
    assert_eq!(owed(&conn).map(|pong| pong.t_echo), Some(sent - 1));

    // The peer starts reading: the latest PING is answered, and nothing else.
    let mut codec = RammuxCodec::new(
        &mut peer_in,
        config().frame_limit,
        Duration::from_secs(3600),
    );
    let mut echoes = Vec::new();
    for _ in 0..100 {
        settle(&mut conn);
        while let Some(Some(frame)) = tokio::task::unconstrained(codec.next()).now_or_never() {
            if let DecodedFrame::Frame(frame) = frame.unwrap() {
                echoes.extend(frame.header.pong.map(|pong| pong.t_echo));
            }
        }
    }
    assert_eq!(echoes.last(), Some(&(sent - 1)));
    // The pipe back holds 1024 bytes while the peer does not read: the answers that fit were written
    // (at most 1024 / PONG frame size of them, rounded up for a frame cut by the pipe) and everything after was superseded, so the rest is the
    // one owed answer. Without the supersession there would be `sent` answers.
    let pong_frame_len = raw(
        Header {
            pong: Some(Pong { t_echo: 0 }),
            ..Default::default()
        },
        b"",
    )
    .len();
    assert!(
        echoes.len() <= 1024usize.div_ceil(pong_frame_len) + 1,
        "at most what fit into the pipe (the last one may be cut by the pipe), plus the owed one: {} answers",
        echoes.len()
    );
    assert!(owed(&conn).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_blocked_writer_keeps_reading_and_owes_one_pong() {
    // The transport does not drain: not even the first frame fits into the pipe back.
    let (read, mut peer_out) = tokio::io::duplex(1 << 20);
    let (write, mut peer_in) = tokio::io::duplex(4);
    let mut conn =
        RammuxConnection::new(RammuxRole::Server, Pipes { read, write }, config()).unwrap();
    let owed = |conn: &RammuxConnection<Pipes>| conn.state.active().unwrap().owed_pong();

    // The first PING rides along with the frame that the writer then gets stuck on.
    // The next ones are read all the same, and only the latest is owed.
    for t_ping in [100, 200, 300] {
        let header = Header {
            ping: Some(t_ping),
            ..Default::default()
        };
        peer_out.write_all(&raw(header, b"")).await.unwrap();
        settle(&mut conn);
        // The frame was read despite the blocked writer.
        if t_ping != 100 {
            assert_eq!(owed(&conn).map(|pong| pong.t_echo), Some(t_ping));
        }
    }

    // The writing resumes: the peer reads, and gets the answer to the latest PING and not to 200.
    let mut codec = RammuxCodec::new(
        &mut peer_in,
        config().frame_limit,
        Duration::from_secs(3600),
    );
    let mut echoes = Vec::new();
    for _ in 0..16 {
        settle(&mut conn);
        while let Some(Some(frame)) = tokio::task::unconstrained(codec.next()).now_or_never() {
            if let DecodedFrame::Frame(frame) = frame.unwrap() {
                echoes.extend(frame.header.pong.map(|pong| pong.t_echo));
            }
        }
    }
    assert_eq!(echoes, [100, 300]);
    assert!(owed(&conn).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_ping_superseded_before_the_next_write_gets_no_pong() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    // Both PINGs are read before the connection writes anything.
    let mut bytes = Vec::new();
    for t_ping in [7, 8] {
        let header = Header {
            ping: Some(t_ping),
            ..Default::default()
        };
        bytes.extend(raw(header, b""));
    }
    rig.peer_out.write_all(&bytes).await.unwrap();
    let pongs = rig.pongs();
    assert_eq!(
        pongs.iter().map(|pong| pong.t_echo).collect::<Vec<_>>(),
        [8]
    );
}

#[tokio::test(start_paused = true)]
async fn a_stream_id_can_be_reused_after_both_sides_closed_it() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    let opening = ControlFlags {
        syn: true,
        fin_write: true,
        ..Default::default()
    };
    let stops_reading = ControlFlags {
        fin_read: true,
        ..Default::default()
    };
    for round in 0..3_u8 {
        // The peer starts the stream, writes and closes its writing half.
        rig.peer_out
            .write_all(&data(0, opening, &[round; 3]))
            .await
            .unwrap();
        let stream = loop {
            match rig.conn.progress().now_or_never() {
                Some(Ok(RammuxProgress::Inbound(stream))) => break stream,
                Some(result) => {
                    result.unwrap();
                },
                None => panic!("the stream with a reused ID was not accepted in round {round}"),
            }
        };
        assert_eq!(rig.conn.stats().inbound_streams, 1);

        // We read everything and close our halves.
        let (sink, mut reader) = stream.into_split();
        assert_eq!(
            reader
                .next()
                .now_or_never()
                .unwrap()
                .unwrap()
                .unwrap()
                .as_ref(),
            [round; 3]
        );
        assert!(reader.next().now_or_never().unwrap().is_none());
        drop(sink);
        drop(reader);
        // The peer stops reading too.
        rig.write(control(0, stops_reading)).await;
        rig.frames();
        assert_eq!(rig.conn.stats().inbound_streams, 0);
    }
}

/// The stream frames among the frames, as (id, credit, data length, flags).
fn stream_frames(frames: &[InboundFrame]) -> Vec<(u16, u32, usize, ControlFlags)> {
    frames
        .iter()
        .filter_map(|frame| {
            let stream = frame.header.stream.as_ref()?;
            Some((
                u16::from_be_bytes(stream.id.to_be_bytes()),
                stream.credit.map_or(0, NonZeroU32::get),
                frame.data.as_ref().len(),
                stream.flags,
            ))
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_window_update_does_not_wait_behind_the_data_of_other_streams() {
    // The pipe back takes 64 bytes while the peer does not read.
    let (read, mut peer_out) = tokio::io::duplex(1 << 20);
    let (write, mut peer_in) = tokio::io::duplex(64);
    let mut small_window = config();
    small_window.local_recv_window = NonZeroU32::new(512).unwrap();
    let mut conn =
        RammuxConnection::new(RammuxRole::Client, Pipes { read, write }, small_window).unwrap();

    // Four streams have four frames of data each, and the transport fills up.
    let mut uploads = Vec::new();
    for _ in 0..4 {
        let mut stream = conn.try_start_outbound().unwrap().unwrap();
        stream
            .feed(Bytes::from(vec![1_u8; 1024]))
            .now_or_never()
            .unwrap()
            .unwrap();
        uploads.push(stream);
    }
    settle(&mut conn);

    // The peer starts a stream, and the application reads enough of it to owe credit.
    peer_out
        .write_all(&data(1, syn(), &[2; 256]))
        .await
        .unwrap();
    let mut download = settle(&mut conn).pop().expect("the stream of the peer");
    assert_eq!(
        download
            .next()
            .now_or_never()
            .unwrap()
            .unwrap()
            .unwrap()
            .len(),
        256
    );

    // The transport drains.
    let mut codec = RammuxCodec::new(
        &mut peer_in,
        config().frame_limit,
        Duration::from_secs(3600),
    );
    let mut frames = Vec::new();
    for _ in 0..1000 {
        while let Some(result) = tokio::task::unconstrained(conn.progress()).now_or_never() {
            result.unwrap();
        }
        while let Some(Some(frame)) = tokio::task::unconstrained(codec.next()).now_or_never() {
            if let DecodedFrame::Frame(frame) = frame.unwrap() {
                frames.push(frame);
            }
        }
    }
    let frames = stream_frames(&frames);
    let window = frames
        .iter()
        .position(|&(id, credit, ..)| id == 1 && credit == 256)
        .expect("the window update");
    let data_before = frames[..window].iter().filter(|frame| frame.2 > 0).count();
    // One frame may have been in the codec already.
    assert!(
        data_before <= 1,
        "{data_before} DATA frames before it: {frames:?}"
    );
    assert_eq!(frames.iter().filter(|frame| frame.2 > 0).count(), 16);
}

#[tokio::test(start_paused = true)]
async fn credit_does_not_starve_data() {
    let mut config = config();
    config.local_recv_window = NonZeroU32::new(512).unwrap();
    let mut rig = rig(RammuxRole::Client, config);
    let mut upload = rig.conn.try_start_outbound().unwrap().unwrap();
    rig.frames();

    // The peer starts 64 streams, with enough data each that reading it owes credit.
    let mut bytes = Vec::new();
    for s in 0..64 {
        bytes.extend(data(2 * s + 1, syn(), &[3; 256]));
    }
    rig.peer_out.write_all(&bytes).await.unwrap();
    // Unconstrained: tokio's budget would make a long run of ready reads look like Pending.
    let mut accepted = Vec::new();
    while let Some(result) = tokio::task::unconstrained(rig.conn.progress()).now_or_never() {
        if let RammuxProgress::Inbound(stream) = result.unwrap() {
            accepted.push(stream);
        }
    }
    assert_eq!(accepted.len(), 64);

    // All of them owe credit at the same moment, and then the upload has data.
    for stream in &mut accepted {
        assert_eq!(
            stream
                .next()
                .now_or_never()
                .unwrap()
                .unwrap()
                .unwrap()
                .len(),
            256
        );
    }
    upload
        .feed(Bytes::from(vec![1_u8; 256]))
        .now_or_never()
        .unwrap()
        .unwrap();

    while let Some(result) = tokio::task::unconstrained(rig.conn.progress()).now_or_never() {
        result.unwrap();
    }
    let mut frames = Vec::new();
    while let Some(Some(frame)) = tokio::task::unconstrained(rig.peer_in.next()).now_or_never() {
        if let DecodedFrame::Frame(frame) = frame.unwrap() {
            frames.push(frame);
        }
    }
    let frames = stream_frames(&frames);
    assert_eq!(frames.len(), 65, "{frames:?}");
    let first_data = frames
        .iter()
        .position(|frame| frame.2 > 0)
        .expect("the DATA frame");
    assert!(first_data <= CREDIT_BURST, "{first_data}");
}

#[tokio::test(start_paused = true)]
async fn a_stream_announces_itself_once() {
    let syn_fin_read = ControlFlags {
        syn: true,
        fin_read: true,
        fin_write: false,
        abort: false,
        reset: false,
    };
    let fin_read = ControlFlags {
        fin_read: true,
        ..Default::default()
    };

    // The reader goes first: SYN rides on FIN_READ.
    let mut one = rig(RammuxRole::Client, config());
    one.frames();
    let (mut sink, reader) = one.conn.try_start_outbound().unwrap().unwrap().into_split();
    drop(reader);
    assert_eq!(stream_frames(&one.frames()), [(0, 0, 0, syn_fin_read)]);
    sink.feed(Bytes::from_static(b"abc"))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(
        stream_frames(&one.frames()),
        [(0, 0, 3, ControlFlags::default())]
    );

    // The writer goes first: SYN rides on DATA.
    let mut two = rig(RammuxRole::Client, config());
    two.frames();
    let (mut sink, reader) = two.conn.try_start_outbound().unwrap().unwrap().into_split();
    sink.feed(Bytes::from_static(b"abc"))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(stream_frames(&two.frames()), [(0, 0, 3, syn())]);
    drop(reader);
    assert_eq!(stream_frames(&two.frames()), [(0, 0, 0, fin_read)]);
}

#[tokio::test(start_paused = true)]
async fn a_sink_dropped_before_close_sends_abort() {
    let abort = ControlFlags {
        fin_write: true,
        abort: true,
        ..Default::default()
    };
    let mut rig = rig(RammuxRole::Client, config());
    rig.frames();
    let (mut sink, _reader) = rig.conn.try_start_outbound().unwrap().unwrap().into_split();
    sink.feed(Bytes::from_static(b"abc"))
        .now_or_never()
        .unwrap()
        .unwrap();
    drop(sink);
    // The connection did not take the data before the drop: it is discarded.
    assert_eq!(
        stream_frames(&rig.frames()),
        [(0, 0, 0, ControlFlags { syn: true, ..abort })]
    );

    // Closing first makes a clean end, even if the close did not complete.
    let (mut sink, _reader) = rig.conn.try_start_outbound().unwrap().unwrap().into_split();
    sink.feed(Bytes::from_static(b"abc"))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert!(sink.close().now_or_never().is_none());
    drop(sink);
    assert_eq!(
        stream_frames(&rig.frames()),
        [(
            2,
            0,
            3,
            ControlFlags {
                syn: true,
                fin_write: true,
                ..Default::default()
            }
        )]
    );
}

#[tokio::test(start_paused = true)]
async fn a_reset_sink_sends_reset() {
    let reset = ControlFlags {
        fin_write: true,
        reset: true,
        ..Default::default()
    };
    let mut rig = rig(RammuxRole::Client, config());
    rig.frames();
    let (mut sink, _reader) = rig.conn.try_start_outbound().unwrap().unwrap().into_split();
    sink.feed(Bytes::from_static(b"abc"))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(stream_frames(&rig.frames()), [(0, 0, 3, syn())]);
    sink.feed(Bytes::from_static(b"def"))
        .now_or_never()
        .unwrap()
        .unwrap();
    // The connection did not take the second write before the reset: it is discarded.
    sink.reset();
    assert_eq!(stream_frames(&rig.frames()), [(0, 0, 0, reset)]);
}

#[tokio::test(start_paused = true)]
async fn a_reset_is_answered_with_fin_read_at_once() {
    let reset = ControlFlags {
        fin_write: true,
        reset: true,
        ..Default::default()
    };
    let fin_read = ControlFlags {
        fin_read: true,
        ..Default::default()
    };
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    rig.peer_out
        .write_all(&data(0, syn(), b"abc"))
        .await
        .unwrap();
    let stream = loop {
        match rig.conn.progress().now_or_never() {
            Some(Ok(RammuxProgress::Inbound(stream))) => break stream,
            Some(result) => {
                result.unwrap();
            },
            None => panic!("the stream was not accepted"),
        }
    };
    assert_eq!(rig.conn.stats().pool_rx_used, 0);
    assert!(stream_frames(&rig.frames()).is_empty());

    // The application holds the stream and reads nothing.
    rig.write(control(0, reset)).await;
    assert_eq!(stream_frames(&rig.frames()), [(0, 0, 0, fin_read)]);
    drop(stream);
}

#[tokio::test(start_paused = true)]
async fn the_answer_to_fin_read_is_a_plain_fin_write() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    rig.peer_out
        .write_all(&data(0, syn(), b"abc"))
        .await
        .unwrap();
    let stream = loop {
        match rig.conn.progress().now_or_never() {
            Some(Ok(RammuxProgress::Inbound(stream))) => break stream,
            Some(result) => {
                result.unwrap();
            },
            None => panic!("the stream was not accepted"),
        }
    };
    let (sink, _reader) = stream.into_split();
    let stops_reading = ControlFlags {
        fin_read: true,
        ..Default::default()
    };
    rig.write(control(0, stops_reading)).await;
    drop(sink);
    assert_eq!(stream_frames(&rig.frames()), [(0, 0, 0, fin_write())]);
}
