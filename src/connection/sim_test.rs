//! Dynamics of the sender on a simulated link, in paused time.
//!
//! The link has a fixed rate and delay in each direction, and an unlimited buffer:
//! whatever the connections put on it queues up, and the queue shows as the round trip time.

use std::{
    collections::VecDeque,
    io,
    num::{NonZeroU16, NonZeroU32},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot},
    time::{Instant, Sleep},
};

use crate::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress, RammuxStats},
    stream::RammuxDuplex,
};

/// One direction of the link.
struct Dir {
    inner: Mutex<DirState>,
    /// Bytes per second.
    rate: f64,
    delay: Duration,
}

#[derive(Default)]
struct DirState {
    /// When the serializer is free again.
    free_at: Option<Instant>,
    /// Bytes on the wire: (arrival, bytes).
    wire: VecDeque<(Instant, Bytes)>,
    /// Bytes waiting to be serialized: (finish, len).
    queue: VecDeque<(Instant, usize)>,
    reader: Option<Waker>,
}

impl Dir {
    fn new(mbit: f64, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: Default::default(),
            rate: mbit * 1e6 / 8.0,
            delay,
        })
    }

    /// Bytes that wait in the buffer of the link, including the one being serialized.
    fn queued(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now();
        while inner
            .queue
            .front()
            .is_some_and(|(finish, _)| *finish <= now)
        {
            inner.queue.pop_front();
        }
        inner.queue.iter().map(|(_, len)| len).sum()
    }
}

/// One end of the link.
struct End {
    tx: Arc<Dir>,
    rx: Arc<Dir>,
    sleep: Pin<Box<Sleep>>,
    /// Delivered, not read yet.
    ready: Bytes,
}

fn link(mbit: f64, one_way: Duration) -> (End, End, Arc<Dir>, Arc<Dir>) {
    let a_to_b = Dir::new(mbit, one_way);
    let b_to_a = Dir::new(mbit, one_way);
    let end = |tx: &Arc<Dir>, rx: &Arc<Dir>| End {
        tx: tx.clone(),
        rx: rx.clone(),
        sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
        ready: Bytes::new(),
    };
    (end(&a_to_b, &b_to_a), end(&b_to_a, &a_to_b), a_to_b, b_to_a)
}

impl AsyncWrite for End {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let dir = &self.tx;
        let mut inner = dir.inner.lock().unwrap();
        let now = Instant::now();
        let start = inner.free_at.map_or(now, |free_at| free_at.max(now));
        let finish = start + Duration::from_secs_f64(buf.len() as f64 / dir.rate);
        inner.free_at = Some(finish);
        inner.queue.push_back((finish, buf.len()));
        inner
            .wire
            .push_back((finish + dir.delay, Bytes::copy_from_slice(buf)));
        if let Some(waker) = inner.reader.take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for End {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.ready.is_empty() {
                let n = this.ready.len().min(buf.remaining());
                buf.put_slice(&this.ready.split_to(n));
                return Poll::Ready(Ok(()));
            }
            let mut inner = this.rx.inner.lock().unwrap();
            match inner.wire.front() {
                Some((arrival, _)) if *arrival <= Instant::now() => {
                    this.ready = inner.wire.pop_front().unwrap().1;
                },
                Some((arrival, _)) => {
                    let arrival = *arrival;
                    this.sleep.as_mut().reset(arrival);
                    drop(inner);
                    let _ = this.sleep.as_mut().poll(cx);
                    return Poll::Pending;
                },
                None => {
                    inner.reader = Some(cx.waker().clone());
                    return Poll::Pending;
                },
            }
        }
    }
}

type Open = oneshot::Sender<RammuxDuplex>;

/// Runs a connection, opens streams on demand, and publishes the stats.
async fn drive(
    mut conn: RammuxConnection<End>,
    mut open: mpsc::UnboundedReceiver<Open>,
    inbound: mpsc::UnboundedSender<RammuxDuplex>,
    stats: Arc<Mutex<RammuxStats>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            progress = conn.progress() => {
                if let RammuxProgress::Inbound(stream) = progress.expect("the connection works") {
                    let _ = inbound.send(stream);
                }
            },
            _ = tick.tick() => {
                *stats.lock().unwrap() = conn.stats();
            },
            Some(tx) = open.recv() => {
                let stream = conn.try_start_outbound().unwrap().expect("a free stream");
                let _ = tx.send(stream);
            },
        }
    }
}

struct Rig {
    open: mpsc::UnboundedSender<Open>,
    client_stats: Arc<Mutex<RammuxStats>>,
    forward: Arc<Dir>,
    /// Bytes read by the server, per stream, in the order of opening.
    read: Vec<Arc<AtomicU64>>,
    server_streams: mpsc::UnboundedReceiver<RammuxDuplex>,
}

impl Rig {
    fn new(mbit: f64, rtt: Duration, config: RammuxConfig) -> Self {
        let (client_io, server_io, forward, _backward) = link(mbit, rtt / 2);
        let client = RammuxConnection::new(RammuxRole::Client, client_io, config.clone()).unwrap();
        let server = RammuxConnection::new(RammuxRole::Server, server_io, config).unwrap();
        let (open, open_rx) = mpsc::unbounded_channel();
        let (_server_open, server_open_rx) = mpsc::unbounded_channel();
        let (client_in, _client_in_rx) = mpsc::unbounded_channel();
        let (server_in, server_streams) = mpsc::unbounded_channel();
        let client_stats = Arc::new(Mutex::new(RammuxStats::default()));
        tokio::spawn(drive(client, open_rx, client_in, client_stats.clone()));
        tokio::spawn(drive(
            server,
            server_open_rx,
            server_in,
            Arc::new(Mutex::new(RammuxStats::default())),
        ));
        std::mem::forget(_server_open);
        std::mem::forget(_client_in_rx);
        Self {
            open,
            client_stats,
            forward,
            read: Vec::new(),
            server_streams,
        }
    }

    /// Starts a bulk upload: the client writes as fast as it can, the server reads
    /// until `stall_at` (never, if [`None`]).
    async fn bulk(&mut self, stall_at: Option<Instant>) {
        self.bulk_writing(stall_at, 64 * 1024).await;
    }

    /// Like [`Self::bulk`], with the given size of one write.
    async fn bulk_writing(&mut self, stall_at: Option<Instant>, write: usize) {
        let (tx, rx) = oneshot::channel();
        self.open.send(tx).unwrap();
        let stream = rx.await.unwrap();
        let (mut sink, _stream) = stream.into_split();
        tokio::spawn(async move {
            let chunk = Bytes::from(vec![5_u8; write]);
            while sink.send(chunk.clone()).await.is_ok() {}
            drop(_stream);
        });

        let server_stream = self.server_streams.recv().await.unwrap();
        let counter = Arc::new(AtomicU64::new(0));
        self.read.push(counter.clone());
        tokio::spawn(async move {
            let (_sink, mut stream) = server_stream.into_split();
            while let Some(data) = stream.next().await {
                counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                if stall_at.is_some_and(|at| Instant::now() >= at) {
                    std::future::pending::<()>().await;
                }
            }
        });
    }

    fn stats(&self) -> RammuxStats {
        self.client_stats.lock().unwrap().clone()
    }

    fn bytes(&self, stream: usize) -> u64 {
        self.read[stream].load(Ordering::Relaxed)
    }

    /// Mbit/s that the server read from the stream while `window` passed.
    async fn rate(&self, stream: usize, window: Duration) -> f64 {
        let before = self.bytes(stream);
        tokio::time::sleep(window).await;
        (self.bytes(stream) - before) as f64 * 8.0 / 1e6 / window.as_secs_f64()
    }
}

const MBIT: f64 = 20.0;
const RTT: Duration = Duration::from_millis(50);

fn config() -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = NonZeroU16::new(16 * 1024).unwrap();
    config.local_recv_window = NonZeroU32::new(32 * 1024).unwrap();
    config.remote_recv_window = NonZeroU32::new(32 * 1024).unwrap();
    config.local_recv_pool = 512 * 1024;
    config.remote_recv_pool = 512 * 1024;
    config.inbound_frame_timeout = Duration::from_secs(3600);
    config
}

#[tokio::test(start_paused = true)]
async fn a_bulk_stream_grows_to_the_pool_and_fills_the_link() {
    let mut rig = Rig::new(MBIT, RTT, config());
    rig.bulk(None).await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    let rate = rig.rate(0, Duration::from_secs(10)).await;
    let stats = rig.stats();
    // Nearly the whole link.
    assert!(rate > 0.9 * MBIT, "{rate} Mbit/s");
    // The stream borrowed (nearly) all of the pool, and there is a queue: the loop is much longer than the path.
    let pool = 512 * 1024;
    assert!(stats.pool_tx_used * 10 >= pool * 9, "{stats:?}");
    assert!(stats.pool_tx_used <= pool, "{stats:?}");
    let srtt = stats.srtt.unwrap();
    assert!(srtt > 3 * RTT, "{srtt:?}");
    // The queue is what the pool and the windows hold beyond the bandwidth-delay product.
    let queued = rig.forward.queued() as f64;
    let expected = (544.0 * 1024.0) - MBIT * 1e6 / 8.0 * RTT.as_secs_f64();
    assert!(
        queued > 0.7 * expected && queued < 1.1 * expected,
        "{queued} {expected}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stalled_reader_stops_borrowing() {
    let mut rig = Rig::new(MBIT, RTT, config());
    // The reader stops early in the ramp, before the stream reached the pool.
    rig.bulk(Some(Instant::now() + Duration::from_millis(150)))
        .await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    let early = rig.stats();
    tokio::time::sleep(Duration::from_secs(20)).await;
    let late = rig.stats();
    assert_eq!(early.pool_tx_used, late.pool_tx_used, "{early:?} {late:?}");
    assert!(late.pool_tx_used < 512 * 1024 / 2, "{late:?}");
    assert!(late.pool_tx_used > 0);
}

#[tokio::test(start_paused = true)]
async fn a_stalled_reader_does_not_hold_up_the_others() {
    let mut rig = Rig::new(MBIT, RTT, config());
    rig.bulk(Some(Instant::now() + Duration::from_millis(150)))
        .await;
    rig.bulk(None).await;

    tokio::time::sleep(Duration::from_secs(8)).await;
    let rate = rig.rate(1, Duration::from_secs(10)).await;
    assert!(rate > 0.85 * MBIT, "{rate} Mbit/s");
}

#[tokio::test(start_paused = true)]
async fn busy_streams_converge_to_equal_shares() {
    let mut rig = Rig::new(MBIT, RTT, config());
    // X holds the whole pool before Y starts.
    rig.bulk(None).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let alone = rig.rate(0, Duration::from_secs(2)).await;
    assert!(alone > 0.9 * MBIT, "{alone}");
    rig.bulk(None).await;

    tokio::time::sleep(Duration::from_secs(12)).await;
    let before = (rig.bytes(0), rig.bytes(1));
    tokio::time::sleep(Duration::from_secs(10)).await;
    let (x, y) = (rig.bytes(0) - before.0, rig.bytes(1) - before.1);
    let total = (x + y) as f64 * 8.0 / 1e6 / 10.0;
    assert!(total > 0.9 * MBIT, "{total}");
    let (low, high) = (x.min(y) as f64, x.max(y) as f64);
    assert!(low / high > 0.8, "x {x} y {y}");
}

/// A new stream starts on its base window and about doubles (at least) its window every loop,
/// like yamux and the adaptive window of HTTP/2, until the pool or the link stops it.
#[tokio::test(start_paused = true)]
async fn a_new_stream_at_least_doubles_per_loop() {
    let mut config = config();
    config.local_recv_pool = 16 * 1024 * 1024;
    config.remote_recv_pool = 16 * 1024 * 1024;
    // A link that is not the limit in the first loops.
    let mut rig = Rig::new(2000.0, RTT, config);
    rig.bulk(None).await;

    let mut last = 0;
    let mut per_loop = Vec::new();
    for _ in 0..6 {
        tokio::time::sleep(RTT).await;
        let now = rig.bytes(0);
        per_loop.push((now - last) as f64);
        last = now;
    }
    // The first loop is the base window.
    assert!(per_loop[0] <= 32.0 * 1024.0 + 1.0, "{per_loop:?}");
    for pair in per_loop.windows(2) {
        assert!(pair[1] >= 1.8 * pair[0], "{per_loop:?}");
    }
}

/// Writes `bytes` to the stream in small writes and closes it, while reading everything the peer writes.
/// Returns the number of bytes read.
async fn talk(stream: RammuxDuplex, bytes: usize) -> usize {
    let (mut sink, mut stream) = stream.into_split();
    let writer = tokio::spawn(async move {
        let mut left = bytes;
        while left > 0 {
            let n = left.min(700);
            sink.send(Bytes::from(vec![7_u8; n])).await.unwrap();
            left -= n;
        }
        sink.close().await.unwrap();
    });
    let mut read = 0;
    while let Some(data) = stream.next().await {
        read += data.len();
    }
    writer.await.unwrap();
    read
}

#[rstest::rstest]
#[case::base_window_of_one_byte_and_no_pool(1, 1_000)]
#[case::base_window_below_a_frame_and_no_pool(100, 30_000)]
#[case::no_pool(64 * 1024, 300_000)]
#[tokio::test(start_paused = true)]
async fn streams_in_both_directions_complete_with_tiny_windows_and_no_pool(
    #[case] window: u32,
    #[case] bytes: usize,
) {
    let mut config = config();
    config.local_recv_window = NonZeroU32::new(window).unwrap();
    config.remote_recv_window = NonZeroU32::new(window).unwrap();
    config.local_recv_pool = 0;
    config.remote_recv_pool = 0;
    let streams = 4;

    let (client_io, server_io, ..) = link(MBIT, RTT / 2);
    let mut done = Vec::new();
    for (role, io) in [
        (RammuxRole::Client, client_io),
        (RammuxRole::Server, server_io),
    ] {
        let conn = RammuxConnection::new(role, io, config.clone()).unwrap();
        let (open, open_rx) = mpsc::unbounded_channel();
        let (inbound, mut inbound_rx) = mpsc::unbounded_channel();
        let stats = Arc::new(Mutex::new(RammuxStats::default()));
        tokio::spawn(drive(conn, open_rx, inbound, stats));
        for _ in 0..streams {
            let (tx, rx) = oneshot::channel();
            open.send(tx).unwrap();
            done.push(tokio::spawn(talk(rx.await.unwrap(), bytes)));
        }
        done.push(tokio::spawn(async move {
            let mut talks = Vec::new();
            for _ in 0..streams {
                talks.push(tokio::spawn(talk(inbound_rx.recv().await.unwrap(), bytes)));
            }
            let mut read = 0;
            for talk in talks {
                read += talk.await.unwrap();
            }
            read
        }));
        std::mem::forget(open);
    }

    let all = futures::future::join_all(done);
    let read = tokio::time::timeout(Duration::from_secs(3000), all)
        .await
        .expect("the streams are stuck");
    let read = read.into_iter().map(Result::unwrap).sum::<usize>();
    // Each side reads `streams` streams of its own and `streams` of the peer's.
    assert_eq!(read, 2 * 2 * streams * bytes);
}

#[tokio::test(start_paused = true)]
async fn the_shares_of_streams_follow_the_size_of_their_writes() {
    // A turn moves one frame, and a frame never holds more than what one write left in the stream.
    // Streams that compete for the pool therefore split it in proportion to their bytes per write
    // (as long as that is below the frame limit): a known limit, see TRANSIT_CONTROL.md.
    let mut rig = Rig::new(MBIT, RTT, config());
    rig.bulk_writing(None, 16 * 1024).await;
    rig.bulk_writing(None, 2 * 1024).await;

    tokio::time::sleep(Duration::from_secs(10)).await;
    let before = (rig.bytes(0), rig.bytes(1));
    tokio::time::sleep(Duration::from_secs(20)).await;
    let (big, small) = (rig.bytes(0) - before.0, rig.bytes(1) - before.1);
    // 16 KiB against 2 KiB per write: about 8 to 1.
    let ratio = big as f64 / small as f64;
    assert!((6.0..10.0).contains(&ratio), "big {big} small {small}");
}
