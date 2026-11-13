//! Tests of the connection against a peer that does random things, many times over.
//!
//! The peer keeps a model of the session as `PROTOCOL.md` describes it, and the application on our side
//! opens, writes, reads and closes streams at random. Everything is driven from fixed seeds, so a failure
//! names the seed that replays it.
//!
//! - A peer that follows the protocol is never rejected, and the connection keeps the protocol too.
//! - A peer that breaks the protocol gets a protocol violation at worst. Nothing it sends makes us panic.

use std::{
    collections::BTreeMap,
    io,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::{
    io::{AsyncWriteExt, DuplexStream},
    task::unconstrained,
};

use crate::{
    RammuxError, StreamId,
    config::{RammuxConfig, RammuxRole},
    connection::{
        Downgraded, RammuxProgress,
        wire_test::{Rig, raw, rig},
    },
    stream::{RammuxSink, RammuxStream},
    wire::{
        codec::decoder::{DecodedFrame, InboundFrame},
        header::{ControlFlags, Header, Pong, StreamHeader},
    },
};

/// How many sessions each test runs.
const SEEDS: u64 = 200;
/// How many things happen in a session.
const STEPS: u64 = 400;

/// Small limits, so that the sessions run into them all the time. The two directions differ,
/// so that mixing them up does not go unnoticed.
fn config() -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(64).unwrap();
    config.max_inbound_streams = 4;
    config.max_outbound_streams = 4;
    config.local_recv_window = NonZeroU32::new(128).unwrap();
    config.remote_recv_window = NonZeroU32::new(96).unwrap();
    config.local_recv_pool = 512;
    config.remote_recv_pool = 384;
    config.ping_bytes = NonZeroU32::new(256).unwrap();
    config.inbound_frame_timeout = Duration::from_secs(3600);
    config
}

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Returns a number from `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// Returns a number from `1..=n`.
    fn up_to(&mut self, n: u64) -> u64 {
        1 + self.below(n)
    }

    fn coin(&mut self) -> bool {
        self.below(2) == 0
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> Option<T> {
        if items.is_empty() {
            return None;
        }
        let at = usize::try_from(self.below(items.len() as u64)).unwrap();
        Some(items[at])
    }

    fn u16_below(&mut self, n: u64) -> u16 {
        u16::try_from(self.below(n)).unwrap()
    }

    fn u32(&mut self) -> u32 {
        u32::try_from(self.next_u64() >> 32).unwrap()
    }
}

/// A stream, as the peer knows it.
#[derive(Default, Debug)]
struct PeerStream {
    peer_fin_write: bool,
    peer_fin_read: bool,
    conn_fin_write: bool,
    conn_fin_read: bool,
    /// Bytes the peer sent, and the connection has not credited back.
    peer_unacked: u64,
    /// Bytes the connection sent, and the peer has not credited back.
    conn_unacked: u64,
}

impl PeerStream {
    /// Whether the stream is closed in both directions on both sides, so that its ID is free.
    fn is_dead(&self) -> bool {
        self.peer_fin_write && self.peer_fin_read && self.conn_fin_write && self.conn_fin_read
    }
}

/// The session, as a peer that follows `PROTOCOL.md` knows it. The peer is the client.
struct Peer {
    config: RammuxConfig,
    streams: BTreeMap<u16, PeerStream>,
    /// `t_ping` of the latest `PING` of the connection.
    last_ping: Option<u32>,
    /// Bytes of `DATA` the connection sent.
    received: u64,
}

impl Peer {
    fn new(config: RammuxConfig) -> Self {
        Self {
            config,
            streams: BTreeMap::new(),
            last_ping: None,
            received: 0,
        }
    }

    /// Bytes of the connection's receive pool that the peer's data uses.
    fn peer_borrowed(&self) -> u64 {
        let base = u64::from(self.config.local_recv_window.get());
        self.streams
            .values()
            .map(|stream| stream.peer_unacked.saturating_sub(base))
            .sum()
    }

    /// Bytes of the peer's receive pool that the connection's data uses.
    fn conn_borrowed(&self) -> u64 {
        let base = u64::from(self.config.remote_recv_window.get());
        self.streams
            .values()
            .map(|stream| stream.conn_unacked.saturating_sub(base))
            .sum()
    }

    /// How much `DATA` the peer may send on the stream in one frame.
    fn data_room(&self, stream: &PeerStream) -> u64 {
        let frame_limit = u64::from(self.config.frame_limit.get());
        if stream.conn_fin_read {
            // Discarded, and not counted.
            return frame_limit;
        }
        let base = u64::from(self.config.local_recv_window.get());
        let pool = u64::from(self.config.local_recv_pool) - self.peer_borrowed();
        (base.saturating_sub(stream.peer_unacked) + pool).min(frame_limit)
    }

    /// Reads a frame of the connection, and checks that the connection keeps the protocol.
    fn observe(&mut self, frame: &InboundFrame) {
        if let Some(t_ping) = frame.header.ping {
            self.last_ping = Some(t_ping);
        }
        let Some(part) = frame.header.stream else {
            return;
        };
        let id = u16::from_be_bytes(part.id.to_be_bytes());
        if part.flags.syn {
            assert_eq!(id % 2, 1, "the server starts streams with odd IDs");
            let previous = self.streams.insert(id, PeerStream::default());
            assert!(previous.is_none(), "SYN for an open stream: {previous:?}");
        }
        let stream = self
            .streams
            .get_mut(&id)
            .unwrap_or_else(|| panic!("frame for a stream that is not open: {frame:?}"));

        if let Some(credit) = part.credit {
            let credit = u64::from(credit.get());
            assert!(!stream.conn_fin_read, "WINDOW after FIN_READ");
            assert!(credit <= stream.peer_unacked, "more credit than unacked");
            stream.peer_unacked -= credit;
        }
        if let Some(len) = part.data_len {
            assert_eq!(frame.data.as_ref().len(), usize::from(len.get()));
            assert!(len <= self.config.frame_limit);
            assert!(!stream.conn_fin_write, "DATA after FIN_WRITE");
            assert!(!stream.peer_fin_read, "DATA after our FIN_READ was read");
            stream.conn_unacked += u64::from(len.get());
            self.received += u64::from(len.get());
        }
        if part.flags.fin_write {
            assert!(!stream.conn_fin_write, "duplicate FIN_WRITE");
            stream.conn_fin_write = true;
        }
        if part.flags.fin_read {
            assert!(!stream.conn_fin_read, "duplicate FIN_READ");
            stream.conn_fin_read = true;
            stream.peer_unacked = 0;
        }
        if stream.is_dead() {
            self.streams.remove(&id);
        }
        assert!(
            self.conn_borrowed() <= u64::from(self.config.remote_recv_pool),
            "the connection exceeded the pool"
        );
    }

    /// Returns a frame that the protocol allows the peer to send now, if the chosen kind has one.
    fn legal_frame(&mut self, rng: &mut Rng) -> Option<Vec<u8>> {
        let ids: Vec<u16> = self.streams.keys().copied().collect();
        match rng.below(12) {
            // Start a stream, maybe with data, maybe closing right away.
            0 | 1 => {
                let started = ids.iter().filter(|id| *id % 2 == 0).count();
                if started >= crate::safe_cast_usize(self.config.max_inbound_streams) {
                    return None;
                }
                let id = rng.u16_below(8) * 2;
                if self.streams.contains_key(&id) {
                    return None;
                }
                let mut stream = PeerStream::default();
                let len = if rng.coin() {
                    rng.up_to(self.data_room(&stream))
                } else {
                    0
                };
                stream.peer_unacked = len;
                stream.peer_fin_write = rng.below(8) == 0;
                let flags = ControlFlags {
                    syn: true,
                    fin_write: stream.peer_fin_write,
                    fin_read: false,
                };
                self.streams.insert(id, stream);
                Some(stream_frame(id, flags, 0, len))
            },

            // Send data, within the base window and the pool.
            2..=5 => {
                let id = rng.pick(&ids)?;
                let room = self.data_room(&self.streams[&id]);
                let stream = self.streams.get_mut(&id)?;
                if stream.peer_fin_write || room == 0 {
                    return None;
                }
                let len = rng.up_to(room);
                if !stream.conn_fin_read {
                    stream.peer_unacked += len;
                }
                Some(stream_frame(id, ControlFlags::default(), 0, len))
            },

            // Return credit.
            6..=8 => {
                let id = rng.pick(&ids)?;
                let stream = self.streams.get_mut(&id)?;
                if stream.peer_fin_read || stream.conn_unacked == 0 {
                    return None;
                }
                let credit = if rng.coin() {
                    stream.conn_unacked
                } else {
                    rng.up_to(stream.conn_unacked)
                };
                stream.conn_unacked -= credit;
                Some(stream_frame(id, ControlFlags::default(), credit, 0))
            },

            // Close a direction.
            9 => {
                let id = rng.pick(&ids)?;
                let stream = self.streams.get_mut(&id)?;
                let mut flags = ControlFlags::default();
                if rng.coin() && !stream.peer_fin_write {
                    stream.peer_fin_write = true;
                    flags.fin_write = true;
                }
                if rng.coin() && !stream.peer_fin_read {
                    stream.peer_fin_read = true;
                    stream.conn_unacked = 0;
                    flags.fin_read = true;
                }
                if stream.is_dead() {
                    self.streams.remove(&id);
                }
                (flags != ControlFlags::default()).then(|| stream_frame(id, flags, 0, 0))
            },

            10 => Some(raw(
                Header {
                    ping: Some(rng.u32()),
                    ..Default::default()
                },
                b"",
            )),

            _ => {
                let t_echo = self.last_ping?;
                Some(raw(
                    Header {
                        pong: Some(Pong { t_echo }),
                        ..Default::default()
                    },
                    b"",
                ))
            },
        }
    }
}

/// Returns a frame that has a legal header, and anything in it.
fn wild_frame(rng: &mut Rng) -> Vec<u8> {
    let mut flags = ControlFlags {
        syn: rng.below(4) == 0,
        fin_write: rng.below(6) == 0,
        fin_read: rng.below(6) == 0,
    };
    let credit = if rng.below(3) == 0 { rng.up_to(600) } else { 0 };
    // Sometimes above the frame limit.
    let len = if rng.coin() { rng.up_to(70) } else { 0 };
    if flags == ControlFlags::default() && credit == 0 && len == 0 {
        flags.syn = true;
    }
    let mut frame = stream_frame(rng.u16_below(12), flags, credit, len);
    if rng.below(8) == 0 {
        // Reserved bits, or flags that do not match the fields that follow.
        frame[0] = u8::try_from(rng.below(256)).unwrap();
    }
    frame
}

fn stream_frame(id: u16, flags: ControlFlags, credit: u64, len: u64) -> Vec<u8> {
    let payload = vec![0xDA; usize::try_from(len).unwrap()];
    let header = Header {
        stream: Some(StreamHeader {
            id: StreamId::from_be_bytes(id.to_be_bytes()),
            flags,
            credit: NonZeroU32::new(u32::try_from(credit).unwrap()),
            data_len: NonZeroU16::new(u16::try_from(len).unwrap()),
        }),
        ..Default::default()
    };
    raw(header, &payload)
}

/// One session: the connection, the application that uses it, and the peer.
struct Session {
    rng: Rng,
    rig: Rig,
    peer: Peer,
    /// The streams the application holds, by their halves.
    app: Vec<(Option<RammuxSink>, Option<RammuxStream>)>,
    /// Bytes of data the application read.
    read: u64,
}

impl Session {
    fn new(seed: u64) -> Self {
        let config = config();
        Self {
            rng: Rng::new(seed),
            rig: rig(RammuxRole::Server, config.clone()),
            peer: Peer::new(config),
            app: Vec::new(),
            read: 0,
        }
    }

    /// Lets the connection do everything it can do now.
    ///
    /// The polls are unconstrained: the session runs in one poll of the test task, and the connection
    /// must not be cut short by the budget of the runtime.
    fn drive(&mut self) -> Result<Option<Downgraded<DuplexStream>>, RammuxError> {
        while let Some(progress) = unconstrained(self.rig.conn.progress()).now_or_never() {
            match progress? {
                RammuxProgress::Inbound(stream) => {
                    let (sink, stream) = stream.into_split();
                    self.app.push((Some(sink), Some(stream)));
                },
                RammuxProgress::Empty => {},
                RammuxProgress::Downgraded(downgraded) => return Ok(Some(downgraded)),
            }
        }
        Ok(None)
    }

    /// Reads what the connection wrote. Returns whether `TERM` was read.
    fn read_frames(&mut self, check: bool) -> bool {
        while let Some(Some(frame)) = unconstrained(self.rig.peer_in.next()).now_or_never() {
            match frame.expect("the connection writes valid frames") {
                DecodedFrame::Frame(frame) if check => self.peer.observe(&frame),
                DecodedFrame::Frame(..) => {},
                DecodedFrame::Terminate => return true,
            }
        }
        false
    }

    async fn peer_writes(&mut self, bytes: &[u8]) {
        self.rig.peer_out.write_all(bytes).await.unwrap();
    }

    /// The application does something with the connection or with one of its streams.
    async fn app_step(&mut self) {
        let at = usize::try_from(self.rng.below(self.app.len().max(1) as u64)).unwrap();
        match self.rng.below(16) {
            0 | 1 => {
                if let Some(stream) = self.rig.conn.try_start_outbound().unwrap() {
                    let (sink, stream) = stream.into_split();
                    self.app.push((Some(sink), Some(stream)));
                }
            },

            2..=5 => {
                if let Some((sink_slot, _)) = self.app.get_mut(at)
                    && let Some(sink) = sink_slot.as_mut()
                {
                    let len = usize::try_from(self.rng.up_to(200)).unwrap();
                    let data = Bytes::from(vec![0xDA; len]);
                    if let Some(Err(error)) = sink.feed(data).now_or_never() {
                        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                        *sink_slot = None;
                    }
                }
            },

            6..=11 => {
                // Read a chunk, or everything there is.
                let everything = self.rng.coin();
                if let Some((_, stream_slot)) = self.app.get_mut(at)
                    && let Some(stream) = stream_slot.as_mut()
                {
                    loop {
                        match stream.next().now_or_never() {
                            Some(Some(data)) if everything => self.read += data.len() as u64,
                            Some(Some(data)) => {
                                self.read += data.len() as u64;
                                break;
                            },
                            Some(None) => {
                                *stream_slot = None;
                                break;
                            },
                            None => break,
                        }
                    }
                }
            },

            12 => {
                // Close writing. Data that is not sent yet goes first.
                if let Some((sink_slot, _)) = self.app.get_mut(at)
                    && let Some(mut sink) = sink_slot.take()
                {
                    let _ = sink.close().now_or_never();
                }
            },

            13 => {
                if let Some((_, stream_slot)) = self.app.get_mut(at) {
                    *stream_slot = None;
                }
            },

            _ => {
                let millis = self.rng.below(50);
                tokio::time::advance(Duration::from_millis(millis)).await;
            },
        }
        self.app
            .retain(|(sink, stream)| sink.is_some() || stream.is_some());
    }

    /// Checks what the connection promises about itself, whatever the peer does.
    fn check_stats(&self) {
        let config = &self.peer.config;
        let stats = self.rig.conn.stats();
        assert!(stats.pool_rx_used <= u64::from(config.local_recv_pool));
        assert!(stats.pool_tx_used <= u64::from(config.remote_recv_pool));
        assert!(stats.advance_held <= stats.pool_tx_used);
        assert!(stats.inbound_streams <= config.max_inbound_streams);
        assert!(stats.outbound_streams <= config.max_outbound_streams);
    }

    /// Drops the connection, if it is still there, and checks that its streams end:
    /// reading reaches the end, and writing fails.
    fn streams_end(self) {
        let Self { rig, mut app, .. } = self;
        drop(rig);
        for (sink, stream) in &mut app {
            if let Some(stream) = stream {
                while stream
                    .next()
                    .now_or_never()
                    .expect("reading does not hang")
                    .is_some()
                {}
            }
            if let Some(sink) = sink {
                let error = sink
                    .send(Bytes::from_static(b"late"))
                    .now_or_never()
                    .expect("writing does not hang")
                    .expect_err("writing fails");
                assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_peer_that_follows_the_protocol_is_never_rejected() {
    let (mut read, mut received) = (0, 0);
    for seed in 0..SEEDS {
        let mut session = Session::new(seed);
        for step in 0..STEPS {
            if session.rng.coin() {
                session.app_step().await;
            } else if let Some(frame) = session.peer.legal_frame(&mut session.rng) {
                session.peer_writes(&frame).await;
            }
            match session.drive() {
                Ok(None) => {},
                Ok(Some(..)) => panic!("seed {seed}, step {step}: downgraded"),
                Err(error) => panic!("seed {seed}, step {step}: {error}"),
            }
            assert!(!session.read_frames(true), "seed {seed}: unexpected TERM");
            session.check_stats();
        }

        // The session ends with a clean downgrade, whatever state the streams are in.
        let term = Header {
            term: true,
            ..Default::default()
        };
        session.peer_writes(&raw(term, b"")).await;
        let downgraded = session
            .drive()
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"))
            .unwrap_or_else(|| panic!("seed {seed}: TERM was not read"));
        unconstrained(downgraded)
            .now_or_never()
            .expect("the transport takes TERM")
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        assert!(session.read_frames(true), "seed {seed}: no TERM");

        read += session.read;
        received += session.peer.received;
        session.streams_end();
    }
    // The sessions did move data, both ways.
    assert!(read > SEEDS * 256, "{read}");
    assert!(received > SEEDS * 512, "{received}");
}

#[tokio::test(start_paused = true)]
async fn a_peer_that_breaks_the_protocol_gets_an_error_and_never_a_panic() {
    let mut rejected = 0;
    for seed in 0..SEEDS {
        let mut session = Session::new(seed);
        // The peer behaves for a while, so that the frame that does not belong meets a session in progress.
        let behaves_for = session.rng.below(STEPS / 2);
        for step in 0..STEPS {
            let behaves = step < behaves_for;
            if session.rng.coin() {
                session.app_step().await;
            } else if !behaves {
                let frame = wild_frame(&mut session.rng);
                session.peer_writes(&frame).await;
            } else if let Some(frame) = session.peer.legal_frame(&mut session.rng) {
                session.peer_writes(&frame).await;
            }

            match session.drive() {
                Ok(None) => {},
                Ok(Some(..)) => panic!("seed {seed}, step {step}: downgraded"),
                Err(error) => {
                    assert!(!behaves, "seed {seed}, step {step}: {error}");
                    assert!(
                        error.is_protocol_violation(),
                        "seed {seed}, step {step}: {error}"
                    );
                    // The failure is final.
                    let error = session.rig.conn.progress().await.unwrap_err();
                    assert!(error.is_user(), "seed {seed}: {error}");
                    rejected += 1;
                    break;
                },
            }
            session.read_frames(behaves);
            session.check_stats();
        }
        session.streams_end();
    }
    // Most wild peers are caught. One that is not sent nothing illegal, or frames we still wait to complete.
    assert!(rejected > SEEDS * 3 / 4, "{rejected}");
}

#[tokio::test(start_paused = true)]
async fn garbage_gets_an_error_and_never_a_panic() {
    for seed in 0..SEEDS * 10 {
        let mut session = Session::new(seed);
        for _ in 0..50 {
            // Mostly zeroes in the high bits, or every frame would fail on its reserved flags.
            let garbage: Vec<u8> = (0..session.rng.up_to(40))
                .map(|_| {
                    let byte = u8::try_from(session.rng.below(256)).unwrap();
                    if session.rng.below(3) == 0 {
                        byte
                    } else {
                        byte & 0x1F
                    }
                })
                .collect();
            session.peer_writes(&garbage).await;
            match session.drive() {
                Ok(None) => session.check_stats(),
                // The garbage happened to be `TERM`.
                Ok(Some(..)) => break,
                Err(error) => {
                    assert!(error.is_protocol_violation(), "seed {seed}: {error}");
                    break;
                },
            }
            session.read_frames(false);
        }
        session.streams_end();
    }
}
