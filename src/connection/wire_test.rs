//! Tests of the connection against a hand-driven peer, frame by frame.

use std::{future::Future, num::NonZeroU16, num::NonZeroU32, time::Duration};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::io::{AsyncWriteExt, DuplexStream, WriteHalf};

use crate::{
    StreamId,
    clock::Clock,
    codec::{
        RammuxCodec,
        decoder::{DecodedFrame, InboundFrame},
    },
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
    header::{ControlFlags, Header, MAX_HEADER_LEN, Pong, StreamHeader},
};

/// A rammux connection and a peer that reads its frames and writes raw bytes to it.
struct Rig {
    conn: RammuxConnection<DuplexStream>,
    peer_in: RammuxCodec<tokio::io::ReadHalf<DuplexStream>>,
    peer_out: WriteHalf<DuplexStream>,
}

fn config() -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(256).unwrap();
    config.ping_bytes = NonZeroU32::new(1024).unwrap();
    config.inbound_frame_timeout = Duration::from_secs(3600);
    config
}

fn rig(role: RammuxRole, config: RammuxConfig) -> Rig {
    let (io_1, io_2) = tokio::io::duplex(1 << 20);
    let (read, write) = tokio::io::split(io_2);
    Rig {
        conn: RammuxConnection::new(role, io_1, config.clone()).unwrap(),
        peer_in: RammuxCodec::new(
            read,
            config.frame_limit,
            Duration::from_secs(3600),
            Clock::new(),
        ),
        peer_out: write,
    }
}

fn raw(header: Header, payload: &[u8]) -> Vec<u8> {
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

    fn global(&mut self) -> &mut crate::global_pool::GlobalPool {
        self.conn
            .state
            .active_mut()
            .unwrap()
            .selector
            .strategy_mut()
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

#[tokio::test(start_paused = true)]
async fn pong_counters_exclude_the_payload_of_the_frame() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    tokio::time::advance(Duration::from_micros(1234)).await;

    // 5 bytes of DATA that carry a PING.
    let header = Header {
        ping: Some(1),
        stream: stream_header(0, syn(), 5),
        ..Default::default()
    };
    rig.write(raw(header, b"hello")).await;
    // Another PING in a frame without data.
    let header = Header {
        ping: Some(2),
        ..Default::default()
    };
    rig.write(raw(header, b"")).await;

    let pongs = rig.pongs();
    assert_eq!(pongs.len(), 2);
    assert_eq!((pongs[0].t_echo, pongs[0].rx_bytes), (1, 0));
    assert_eq!((pongs[1].t_echo, pongs[1].rx_bytes), (2, 5));
    assert_eq!(pongs[0].t_read, 1234);
    assert_eq!(pongs[1].t_read, 1234);
}

#[tokio::test(start_paused = true)]
async fn rx_bytes_wraps() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    rig.global().rx_bytes = u32::MAX - 2;

    let header = Header {
        stream: stream_header(0, syn(), 5),
        ..Default::default()
    };
    rig.write(raw(header, b"hello")).await;
    let header = Header {
        ping: Some(1),
        ..Default::default()
    };
    rig.write(raw(header, b"")).await;
    assert_eq!(rig.pongs()[0].rx_bytes, 2);
}

#[tokio::test(start_paused = true)]
async fn rx_bytes_counts_data_the_reader_never_sees() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    // The stream is opened and immediately dropped by `settle`, which discards the new stream.
    let header = Header {
        stream: stream_header(0, syn(), 3),
        ..Default::default()
    };
    rig.write(raw(header, b"abc")).await;
    let header = Header {
        stream: stream_header(0, ControlFlags::default(), 4),
        ping: Some(1),
        ..Default::default()
    };
    rig.write(raw(header, b"defg")).await;
    let header = Header {
        ping: Some(2),
        ..Default::default()
    };
    rig.write(raw(header, b"")).await;
    let pongs = rig.pongs();
    assert_eq!(pongs.last().unwrap().rx_bytes, 7);
}

/// A PONG that carries the given echo.
fn pong_of(t_echo: u32) -> Vec<u8> {
    raw(
        Header {
            pong: Some(Pong {
                t_echo,
                t_read: 0,
                rx_bytes: 0,
            }),
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
async fn only_a_pong_for_a_ping_sent_after_the_last_answered_is_a_sample() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    let first = rig.send_ping(Duration::from_secs(5)).await;
    tokio::time::advance(Duration::from_millis(5)).await;
    rig.write(pong_of(first)).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));

    let second = rig.send_ping(Duration::from_secs(5)).await;
    tokio::time::advance(Duration::from_millis(20)).await;
    // A duplicate of an answered echo is ignored.
    rig.write(pong_of(first)).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));
    // An echo of a stamp we never sent is ignored.
    rig.write(pong_of(second + 1000)).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));
    // The genuine answer counts.
    rig.write(pong_of(second)).await;
    assert!(rig.conn.stats().srtt.unwrap() > Duration::from_millis(5));
    let srtt = rig.conn.stats().srtt;
    // Now it is a duplicate, too.
    rig.write(pong_of(second)).await;
    assert_eq!(rig.conn.stats().srtt, srtt);
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
async fn the_reserved_bit_is_a_protocol_violation() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    // PING with bit 0x0080 set.
    rig.peer_out
        .write_all(&[0x00, 0xA0, 0, 0, 0, 1])
        .await
        .unwrap();
    let error = rig.conn.progress().await.err().unwrap();
    assert!(error.is_protocol_violation());
}

#[tokio::test(start_paused = true)]
async fn pings_follow_the_data_and_the_clock() {
    let mut rig = rig(RammuxRole::Client, config());
    let stream = rig.conn.try_start_outbound().unwrap().unwrap();
    let (mut sink, _stream) = stream.into_split();

    let mut frames = Vec::new();
    for _ in 0..10 {
        let chunk = Bytes::from(vec![7_u8; 1024]);
        driving(&mut rig.conn, async {
            sink.send(chunk).await.unwrap();
            sink.flush().await.unwrap();
        })
        .await;
        frames.extend(rig.frames());
    }
    let data = frames
        .iter()
        .map(|frame| frame.data.as_ref().len())
        .sum::<usize>();
    assert_eq!(data, 10 * 1024);
    let stamps = frames
        .iter()
        .filter_map(|frame| frame.header.ping)
        .collect::<Vec<_>>();
    // One at the start, then one per 1024 bytes.
    assert!((10..=12).contains(&stamps.len()), "{stamps:?}");
    assert!(
        stamps
            .windows(2)
            .all(|w| crate::clock::diff(w[1], w[0]) > 0)
    );

    // No data, so the clock decides.
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|f| f.header.ping.is_some())
            .count(),
        0
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|f| f.header.ping.is_some())
            .count(),
        1
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|f| f.header.ping.is_some())
            .count(),
        1
    );
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
    // DATA after FIN_WRITE, a second FIN_WRITE.
    let fw = ControlFlags {
        syn: true,
        ..fin_write()
    };
    violation(config(), vec![data(0, fw, b"x"), data(0, none, b"x")]).await;
    violation(config(), vec![data(0, fw, b"x"), control(0, fin_write())]).await;
    // A second FIN_READ, WINDOW after FIN_READ.
    let fr = ControlFlags {
        fin_read: true,
        ..none
    };
    violation(config(), vec![control(0, fin_read()), control(0, fr)]).await;
    let window = raw(credit_header(0, none, 1), b"");
    violation(config(), vec![control(0, fin_read()), window]).await;
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

impl tokio::io::AsyncRead for Pipes {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for Pipes {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn a_flood_of_pings_is_read_without_a_stall_and_answered_with_the_latest() {
    // A peer that sends only PINGs and does not read for a while. The answers do not fit the pipe
    // back, but the connection must keep reading, and hold at most one answer.
    let (read, mut peer_out) = tokio::io::duplex(1 << 20);
    let (write, mut peer_in) = tokio::io::duplex(1024);
    let mut conn =
        RammuxConnection::new(RammuxRole::Server, Pipes { read, write }, config()).unwrap();
    let owed = |conn: &RammuxConnection<Pipes>| conn.state.active().unwrap().pong;
    let ping = |t_ping| {
        raw(
            Header {
                ping: Some(t_ping),
                ..Default::default()
            },
            b"",
        )
    };
    // Unconstrained: tokio's budget would make a long run of ready reads look like Pending.
    let settle = |conn: &mut RammuxConnection<Pipes>| {
        while let Some(result) = tokio::task::unconstrained(conn.progress()).now_or_never() {
            result.unwrap();
        }
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
        Clock::new(),
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
    assert!(
        echoes.len() < 100,
        "far fewer answers than PINGs: {echoes:?}"
    );
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
            reader.next().now_or_never().unwrap().unwrap().as_ref(),
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
