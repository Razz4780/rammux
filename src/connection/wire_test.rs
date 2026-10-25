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

    fn now_micros(&self) -> u32 {
        let clock = self.conn.state.active().unwrap().clock;
        clock.micros(tokio::time::Instant::now())
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
async fn every_ping_is_answered_once_in_order() {
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

#[tokio::test(start_paused = true)]
async fn pong_gives_the_round_trip_time() {
    let mut rig = rig(RammuxRole::Server, config());
    rig.frames();
    assert_eq!(rig.conn.stats().srtt, None);

    tokio::time::advance(Duration::from_millis(50)).await;
    let t_echo = rig.now_micros() - 5000;
    let header = Header {
        pong: Some(Pong {
            t_echo,
            t_read: 0,
            rx_bytes: 0,
        }),
        ..Default::default()
    };
    // A PONG is welcome although we have no PING in flight.
    rig.write(raw(header, b"")).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));

    // A PONG from the future is ignored.
    let header = Header {
        pong: Some(Pong {
            t_echo: rig.now_micros() + 1000,
            t_read: 0,
            rx_bytes: 0,
        }),
        ..Default::default()
    };
    rig.write(raw(header, b"")).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));
}

#[tokio::test(start_paused = true)]
async fn a_round_trip_longer_than_the_frame_timeout_is_not_a_sample() {
    // No PONG can come back later than the peer is allowed to stay silent: a sample that large comes
    // from a stale or forged t_echo, and would push the smoothed round trip time to minutes.
    let mut config = config();
    config.inbound_frame_timeout = Duration::from_secs(10);
    let mut rig = rig(RammuxRole::Server, config);
    rig.frames();
    let pong = |t_echo| Header {
        pong: Some(Pong {
            t_echo,
            t_read: 0,
            rx_bytes: 0,
        }),
        ..Default::default()
    };
    tokio::time::advance(Duration::from_millis(50)).await;
    rig.write(raw(pong(rig.now_micros() - 5000), b"")).await;
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));

    // The frame is already there when the time passes.
    let t_echo = rig.now_micros();
    rig.peer_out
        .write_all(&raw(pong(t_echo), b""))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(10) + Duration::from_millis(1)).await;
    rig.settle();
    assert_eq!(rig.conn.stats().srtt, Some(Duration::from_millis(5)));
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
async fn a_flood_of_pings_does_not_grow_the_owed_pongs_without_bound() {
    // A peer that sends only PINGs and never reads. The answers do not fit the pipe back, so the
    // connection must stop reading at the cap instead of queueing a PONG for every PING.
    let (read, mut peer_out) = tokio::io::duplex(1 << 20);
    let (write, mut peer_in) = tokio::io::duplex(1024);
    let mut conn =
        RammuxConnection::new(RammuxRole::Server, Pipes { read, write }, config()).unwrap();
    let owed = |conn: &RammuxConnection<Pipes>| conn.state.active().unwrap().pongs.len();
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
    assert_eq!(owed(&conn), super::MAX_OWED_PONGS);

    // The peer starts reading: the connection resumes, and every PING is answered in the end.
    let mut codec = RammuxCodec::new(
        &mut peer_in,
        config().frame_limit,
        Duration::from_secs(3600),
        Clock::new(),
    );
    let mut pongs = 0;
    while pongs < sent {
        settle(&mut conn);
        while let Some(Some(frame)) = tokio::task::unconstrained(codec.next()).now_or_never() {
            if let DecodedFrame::Frame(frame) = frame.unwrap() {
                pongs += u32::from(frame.header.pong.is_some());
            }
        }
        assert!(owed(&conn) <= super::MAX_OWED_PONGS);
    }
    assert_eq!(owed(&conn), 0);
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
