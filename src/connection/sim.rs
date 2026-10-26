//! A simulated link and a rig of two connections over it, in paused tokio time.
//!
//! Each direction of the link is a FIFO with a rate and a propagation delay that can change
//! over time, an unlimited buffer by default (whatever is put on the link queues up, and the queue
//! shows as delay), and optional jitter. Traffic of other senders can share the FIFO.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

use std::{
    collections::VecDeque,
    io,
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
    clock::Clock,
    config::{RammuxConfig, RammuxRole},
    connection::{DelayControlStats, RammuxConnection, RammuxProgress, RammuxStats},
    stream::RammuxDuplex,
};

/// Bytes that every write puts on the wire besides its payload: Ethernet, IP and TCP headers.
/// A write is a segment, so a frame of a few bytes costs as much as a frame of a kilobyte.
pub const WIRE_HEADER: usize = 54;

/// A value that depends on the time since the start of the simulation.
pub type OfTime<T> = Arc<dyn Fn(Duration) -> T + Send + Sync>;

/// Parameters of one direction of the link.
#[derive(Clone)]
pub struct Path {
    /// Rate in Mbit/s.
    pub mbit: OfTime<f64>,
    /// Propagation delay.
    pub delay: OfTime<Duration>,
    /// Extra random delay in `[0, jitter)` per packet. The order of packets is kept.
    pub jitter: Duration,
    /// Buffer limit in bytes for packets of foreign senders (they are dropped above it).
    /// Packets of the connections are never dropped.
    pub foreign_buffer: usize,
}

impl Path {
    pub fn constant(mbit: f64, delay: Duration) -> Self {
        Self {
            mbit: Arc::new(move |_| mbit),
            delay: Arc::new(move |_| delay),
            jitter: Duration::ZERO,
            foreign_buffer: usize::MAX,
        }
    }

    pub fn with_rate(mut self, mbit: impl Fn(Duration) -> f64 + Send + Sync + 'static) -> Self {
        self.mbit = Arc::new(mbit);
        self
    }

    pub fn with_delay(
        mut self,
        delay: impl Fn(Duration) -> Duration + Send + Sync + 'static,
    ) -> Self {
        self.delay = Arc::new(delay);
        self
    }

    /// Packets of foreign senders are dropped when more than `bytes` are queued.
    pub fn with_foreign_buffer(mut self, bytes: usize) -> Self {
        self.foreign_buffer = bytes;
        self
    }

    pub fn with_jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }
}

enum Owner {
    /// Bytes of a connection, for the reader on the given wire.
    Connection(usize, Bytes),
    /// A packet of a foreign sender.
    Foreign,
}

struct Item {
    len: usize,
    remaining: f64,
    owner: Owner,
}

/// What reaches one reader.
struct Wire {
    /// More propagation delay for this connection than for the others, in this direction.
    extra_delay: Duration,
    last_arrival: Instant,
    /// Bytes on the wire: (arrival, bytes).
    bytes: VecDeque<(Instant, Bytes)>,
    reader: Option<Waker>,
}

struct DirState {
    queue: VecDeque<Item>,
    queued_bytes: f64,
    /// The time up to which the queue was served.
    served_to: Instant,
    /// One wire per connection that uses this direction.
    wires: Vec<Wire>,
    /// Foreign packets that went through: (time the sender sees the ack, len).
    foreign_acks: VecDeque<(Instant, usize)>,
    rng: u64,
    /// Everything served so far, in bytes.
    served_bytes: f64,
    /// Writes of the connections, and their payload in bytes.
    writes: u64,
    written_bytes: u64,
}

/// One direction of the link.
pub struct Dir {
    inner: Mutex<DirState>,
    path: Path,
    start: Instant,
    /// The delay of the way back, for the acknowledgements of foreign packets.
    back_delay: Mutex<Duration>,
}

impl Dir {
    fn new(path: Path) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            inner: Mutex::new(DirState {
                queue: VecDeque::new(),
                queued_bytes: 0.0,
                served_to: now,
                wires: Vec::new(),
                foreign_acks: VecDeque::new(),
                rng: 0x2545_F491_4F6C_DD1D,
                served_bytes: 0.0,
                writes: 0,
                written_bytes: 0,
            }),
            path,
            start: now,
            back_delay: Mutex::new(Duration::ZERO),
        })
    }

    fn rate(&self, at: Instant) -> f64 {
        (self.path.mbit)(at.saturating_duration_since(self.start)) * 1e6 / 8.0
    }

    /// Serves the queue up to `now`.
    fn advance(&self, state: &mut DirState, now: Instant) {
        let back = *self.back_delay.lock().unwrap();
        while state.served_to < now {
            let Some(head) = state.queue.front_mut() else {
                state.served_to = now;
                break;
            };
            let rate = self.rate(state.served_to).max(1.0);
            let available = rate * (now - state.served_to).as_secs_f64();
            if head.remaining > available {
                head.remaining -= available;
                state.queued_bytes -= available;
                state.served_bytes += available;
                state.served_to = now;
                break;
            }
            let took = Duration::from_secs_f64(head.remaining / rate);
            state.served_to += took;
            state.queued_bytes -= head.remaining;
            state.served_bytes += head.remaining;
            let finished = state.served_to;
            let item = state.queue.pop_front().expect("the head is there");
            let mut arrival =
                finished + (self.path.delay)(finished.saturating_duration_since(self.start));
            if !self.path.jitter.is_zero() {
                state.rng ^= state.rng << 13;
                state.rng ^= state.rng >> 7;
                state.rng ^= state.rng << 17;
                let fraction = (state.rng % 10_000) as f64 / 10_000.0;
                arrival += self.path.jitter.mul_f64(fraction);
            }
            match item.owner {
                Owner::Connection(wire, bytes) => {
                    let wire = &mut state.wires[wire];
                    arrival += wire.extra_delay;
                    arrival = arrival.max(wire.last_arrival);
                    wire.last_arrival = arrival;
                    wire.bytes.push_back((arrival, bytes));
                },
                Owner::Foreign => state.foreign_acks.push_back((arrival + back, item.len)),
            }
        }
    }

    /// Bytes in the queue now, the one in service included.
    pub fn queued(&self) -> usize {
        let mut state = self.inner.lock().unwrap();
        self.advance(&mut state, Instant::now());
        state.queued_bytes.max(0.0) as usize
    }

    /// The delay of a byte put on the link now, from the queue alone.
    pub fn queue_delay(&self) -> Duration {
        let rate = self.rate(Instant::now());
        Duration::from_secs_f64(self.queued() as f64 / rate)
    }

    /// Bytes served so far.
    pub fn served(&self) -> f64 {
        let mut state = self.inner.lock().unwrap();
        self.advance(&mut state, Instant::now());
        state.served_bytes
    }

    pub fn capacity_now(&self) -> f64 {
        self.rate(Instant::now())
    }

    /// Puts a packet of a foreign sender on the link. Returns whether it was accepted.
    pub fn push_foreign(&self, len: usize) -> bool {
        let mut state = self.inner.lock().unwrap();
        self.advance(&mut state, Instant::now());
        if state.queued_bytes as usize + len > self.path.foreign_buffer {
            return false;
        }
        state.queue.push_back(Item {
            len,
            remaining: (len + WIRE_HEADER) as f64,
            owner: Owner::Foreign,
        });
        state.queued_bytes += (len + WIRE_HEADER) as f64;
        true
    }

    /// The number of writes of the connections so far (each is a segment on the wire), and the bytes of
    /// payload in them.
    pub fn writes(&self) -> (u64, u64) {
        let state = self.inner.lock().unwrap();
        (state.writes, state.written_bytes)
    }

    /// Takes the bytes of foreign packets that are acknowledged by now.
    pub fn take_foreign_acks(&self) -> usize {
        let mut state = self.inner.lock().unwrap();
        let now = Instant::now();
        let mut total = 0;
        while state.foreign_acks.front().is_some_and(|(at, _)| *at <= now) {
            total += state.foreign_acks.pop_front().unwrap().1;
        }
        total
    }
}

/// One end of the link.
pub struct End {
    tx: Arc<Dir>,
    /// The wire of the reader on the other end.
    tx_wire: usize,
    rx: Arc<Dir>,
    /// Our wire.
    rx_wire: usize,
    sleep: Pin<Box<Sleep>>,
    ready: Bytes,
}

/// The two directions of a bottleneck, which any number of connections can share.
#[derive(Clone)]
pub struct Links {
    /// The direction from the clients to the servers.
    pub forward: Arc<Dir>,
    /// The direction from the servers to the clients.
    pub backward: Arc<Dir>,
}

impl Links {
    pub fn new(client_to_server: Path, server_to_client: Path) -> Self {
        let forward = Dir::new(client_to_server.clone());
        let backward = Dir::new(server_to_client.clone());
        // Foreign packets are acknowledged on the way back, which takes about its propagation delay.
        *forward.back_delay.lock().unwrap() = (server_to_client.delay)(Duration::ZERO);
        *backward.back_delay.lock().unwrap() = (client_to_server.delay)(Duration::ZERO);
        // The queues are served while nobody looks, so that a reader finds its data on time.
        for dir in [&forward, &backward] {
            let dir = dir.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(1));
                loop {
                    tick.tick().await;
                    let mut state = dir.inner.lock().unwrap();
                    dir.advance(&mut state, Instant::now());
                    let now = Instant::now();
                    for wire in &mut state.wires {
                        if wire.bytes.front().is_some_and(|(at, _)| *at <= now)
                            && let Some(waker) = wire.reader.take()
                        {
                            waker.wake();
                        }
                    }
                }
            });
        }
        Self { forward, backward }
    }

    /// The ends of a new connection, which has `extra` more propagation delay in each direction.
    pub fn pair_with_extra_delay(&self, extra: Duration) -> (End, End) {
        let wire = |dir: &Arc<Dir>| {
            let mut state = dir.inner.lock().unwrap();
            state.wires.push(Wire {
                extra_delay: extra,
                last_arrival: Instant::now(),
                bytes: VecDeque::new(),
                reader: None,
            });
            state.wires.len() - 1
        };
        let (to_server, to_client) = (wire(&self.forward), wire(&self.backward));
        let end = |tx: &Arc<Dir>, tx_wire, rx: &Arc<Dir>, rx_wire| End {
            tx: tx.clone(),
            tx_wire,
            rx: rx.clone(),
            rx_wire,
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            ready: Bytes::new(),
        };
        (
            end(&self.forward, to_server, &self.backward, to_client),
            end(&self.backward, to_client, &self.forward, to_server),
        )
    }
}

impl End {
    /// Puts one segment on the link.
    fn send(&self, payload: Bytes) {
        let dir = &self.tx;
        let mut state = dir.inner.lock().unwrap();
        dir.advance(&mut state, Instant::now());
        let cost = payload.len() + WIRE_HEADER;
        state.writes += 1;
        state.written_bytes += payload.len() as u64;
        state.queue.push_back(Item {
            len: payload.len(),
            remaining: cost as f64,
            owner: Owner::Connection(self.tx_wire, payload),
        });
        state.queued_bytes += cost as f64;
    }
}

impl AsyncWrite for End {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.send(Bytes::copy_from_slice(buf));
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        // A vectored write is one segment, like a writev on a socket.
        let mut payload = Vec::with_capacity(bufs.iter().map(|buf| buf.len()).sum());
        for buf in bufs {
            payload.extend_from_slice(buf);
        }
        let len = payload.len();
        self.send(payload.into());
        Poll::Ready(Ok(len))
    }

    fn is_write_vectored(&self) -> bool {
        true
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
            let mut state = this.rx.inner.lock().unwrap();
            let wire = &mut state.wires[this.rx_wire];
            match wire.bytes.front() {
                Some((arrival, _)) if *arrival <= Instant::now() => {
                    this.ready = wire.bytes.pop_front().unwrap().1;
                },
                Some((arrival, _)) => {
                    let arrival = *arrival;
                    this.sleep.as_mut().reset(arrival);
                    drop(state);
                    let _ = this.sleep.as_mut().poll(cx);
                    return Poll::Pending;
                },
                None => {
                    wire.reader = Some(cx.waker().clone());
                    return Poll::Pending;
                },
            }
        }
    }
}

pub type Open = oneshot::Sender<RammuxDuplex>;

/// Runs a connection, opens streams on demand, and publishes the stats.
pub async fn drive(
    mut conn: RammuxConnection<End>,
    mut open: mpsc::UnboundedReceiver<Open>,
    inbound: mpsc::UnboundedSender<RammuxDuplex>,
    stats: Arc<Mutex<RammuxStats>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(10));
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

/// How the application of a stream behaves.
#[derive(Clone)]
pub struct App {
    /// Size of one write.
    pub write_size: usize,
    /// Whether to flush after every write.
    pub flush: bool,
    /// Pause of the reader after every read.
    pub read_delay: Duration,
    /// The reader stops reading at this time.
    pub stall_at: Option<Instant>,
    /// The writer sends this many bytes per second at most. [`None`]: as fast as it can.
    pub rate: Option<f64>,
    /// The writer is silent in this window of the simulation time: (from, until).
    pub pause: Option<(Duration, Duration)>,
    /// The writer starts at this time.
    pub starts: Duration,
}

impl Default for App {
    fn default() -> Self {
        Self {
            write_size: 64 * 1024,
            flush: true,
            read_delay: Duration::ZERO,
            stall_at: None,
            rate: None,
            pause: None,
            starts: Duration::ZERO,
        }
    }
}

/// Counters of one stream, for each direction.
#[derive(Default)]
pub struct Counters {
    /// Bytes the server read.
    pub up: AtomicU64,
    /// Bytes the client read.
    pub down: AtomicU64,
}

pub struct Rig {
    open: mpsc::UnboundedSender<Open>,
    pub client_stats: Arc<Mutex<RammuxStats>>,
    /// The direction from the client to the server.
    pub forward: Arc<Dir>,
    /// The direction from the server to the client.
    pub backward: Arc<Dir>,
    pub streams: Vec<Arc<Counters>>,
    server_streams: mpsc::UnboundedReceiver<RammuxDuplex>,
    start: Instant,
}

/// A clock that runs fast or slow: `ppm` parts per million.
pub fn skewed_clock(ppm: i64) -> Clock {
    Clock::skewed(ppm)
}

impl Rig {
    /// A symmetric link.
    pub fn new(mbit: f64, rtt: Duration, config: RammuxConfig) -> Self {
        let path = Path::constant(mbit, rtt / 2);
        Self::on(&Links::new(path.clone(), path), config.clone(), config, 0)
    }

    /// A connection on a link of its own. `client_to_server` and `server_to_client` are the
    /// two directions.
    pub fn with_paths(
        client_to_server: Path,
        server_to_client: Path,
        client_config: RammuxConfig,
        server_config: RammuxConfig,
        server_ppm: i64,
    ) -> Self {
        let links = Links::new(client_to_server, server_to_client);
        Self::on(&links, client_config, server_config, server_ppm)
    }

    /// A connection on the given link, which other connections can share.
    /// `server_ppm` makes the clock of the server run fast (positive) or slow.
    pub fn on(
        links: &Links,
        client_config: RammuxConfig,
        server_config: RammuxConfig,
        server_ppm: i64,
    ) -> Self {
        Self::on_with_extra_delay(
            links,
            client_config,
            server_config,
            server_ppm,
            Duration::ZERO,
        )
    }

    /// Like [`Self::on`], with `extra` more propagation delay in each direction.
    pub fn on_with_extra_delay(
        links: &Links,
        client_config: RammuxConfig,
        server_config: RammuxConfig,
        server_ppm: i64,
        extra: Duration,
    ) -> Self {
        let (client_io, server_io) = links.pair_with_extra_delay(extra);
        let (forward, backward) = (links.forward.clone(), links.backward.clone());
        let client = RammuxConnection::new(RammuxRole::Client, client_io, client_config).unwrap();
        let server = RammuxConnection::with_clock(
            RammuxRole::Server,
            server_io,
            server_config,
            skewed_clock(server_ppm),
        )
        .unwrap();
        let (open, open_rx) = mpsc::unbounded_channel();
        let (server_open, server_open_rx) = mpsc::unbounded_channel();
        let (client_in, client_in_rx) = mpsc::unbounded_channel();
        let (server_in, server_streams) = mpsc::unbounded_channel();
        let client_stats = Arc::new(Mutex::new(RammuxStats::default()));
        let server_stats = Arc::new(Mutex::new(RammuxStats::default()));
        tokio::spawn(drive(client, open_rx, client_in, client_stats.clone()));
        tokio::spawn(drive(
            server,
            server_open_rx,
            server_in,
            server_stats.clone(),
        ));
        std::mem::forget(server_open);
        std::mem::forget(client_in_rx);
        Self {
            open,
            client_stats,
            forward,
            backward,
            streams: Vec::new(),
            server_streams,
            start: Instant::now(),
        }
    }

    /// Starts a stream. `upload` is the application that writes at the client and is read by the
    /// server, `download` the other way around. [`None`]: nobody writes in that direction.
    pub async fn stream(&mut self, upload: Option<App>, download: Option<App>) -> usize {
        let (tx, rx) = oneshot::channel();
        self.open.send(tx).unwrap();
        let client = rx.await.unwrap();
        let (mut client_sink, client_stream) = client.into_split();
        let counters = Arc::new(Counters::default());
        self.streams.push(counters.clone());
        let start = self.start;

        // The client writes, or at least says that the stream exists (one byte), so that the
        // server accepts it.
        let up_app = upload.clone();
        tokio::spawn(async move {
            match up_app {
                Some(app) => {
                    let chunk = Bytes::from(vec![5_u8; app.write_size]);
                    write_loop(&mut client_sink, chunk, app, start).await;
                },
                None => {
                    let _ = client_sink.send(Bytes::from_static(b"x")).await;
                    std::future::pending::<()>().await;
                },
            }
        });
        let down_app = download.clone().unwrap_or_default();
        let down_counter = counters.clone();
        tokio::spawn(read_loop(client_stream, down_app, move |n| {
            down_counter.down.fetch_add(n, Ordering::Relaxed);
        }));

        let server_stream = self.server_streams.recv().await.unwrap();
        let (mut server_sink, server_stream) = server_stream.into_split();
        let up_counter = counters;
        let up_app = upload.unwrap_or_default();
        let counts_up = !matches!(&up_app, App { write_size: 0, .. });
        tokio::spawn(read_loop(server_stream, up_app, move |n| {
            if counts_up {
                up_counter.up.fetch_add(n, Ordering::Relaxed);
            }
        }));
        if let Some(app) = download {
            let chunk = Bytes::from(vec![6_u8; app.write_size]);
            tokio::spawn(async move {
                write_loop(&mut server_sink, chunk, app, start).await;
            });
        } else {
            tokio::spawn(async move {
                let _keep = server_sink;
                std::future::pending::<()>().await;
            });
        }
        self.streams.len() - 1
    }

    /// A bulk upload from the client, read by an application that does not stall.
    pub async fn bulk(&mut self, stall_at: Option<Instant>) -> usize {
        self.bulk_app(App {
            stall_at,
            ..Default::default()
        })
        .await
    }

    pub async fn bulk_app(&mut self, app: App) -> usize {
        self.stream(Some(app), None).await
    }

    pub fn stats(&self) -> RammuxStats {
        self.client_stats.lock().unwrap().clone()
    }

    /// The `DelayControl` statistics of the client. Panics without `DelayControl`.
    pub fn delay_stats(&self) -> DelayControlStats {
        self.stats()
            .delay_control
            .expect("the client runs DelayControl and has sampled its statistics")
    }

    /// Bytes read by the server on the stream.
    pub fn bytes(&self, stream: usize) -> u64 {
        self.streams[stream].up.load(Ordering::Relaxed)
    }

    /// Mbit/s that the server read from the stream while `window` passed.
    pub async fn rate(&self, stream: usize, window: Duration) -> f64 {
        let before = self.bytes(stream);
        tokio::time::sleep(window).await;
        (self.bytes(stream) - before) as f64 * 8.0 / 1e6 / window.as_secs_f64()
    }
}

/// Reads like the application says, and tells how many bytes it read.
async fn read_loop(mut stream: crate::stream::RammuxStream, app: App, count: impl Fn(u64)) {
    while let Some(data) = stream.next().await {
        count(data.len() as u64);
        if !app.read_delay.is_zero() {
            tokio::time::sleep(app.read_delay).await;
        }
        if app.stall_at.is_some_and(|at| Instant::now() >= at) {
            std::future::pending::<()>().await;
        }
    }
}

async fn write_loop<S>(sink: &mut S, chunk: Bytes, app: App, start: Instant)
where
    S: futures::Sink<Bytes> + Unpin,
{
    if !app.starts.is_zero() {
        tokio::time::sleep(app.starts).await;
    }
    // The application with a rate sends a chunk when its budget allows.
    let mut credit = 0.0_f64;
    let mut last = Instant::now();
    loop {
        if let Some((from, until)) = app.pause {
            let t = Instant::now().saturating_duration_since(start);
            if t >= from && t < until {
                tokio::time::sleep(until - t).await;
                last = Instant::now();
                credit = 0.0;
            }
        }
        if let Some(rate) = app.rate {
            let now = Instant::now();
            credit += rate * (now - last).as_secs_f64();
            last = now;
            if credit < chunk.len() as f64 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            credit -= chunk.len() as f64;
        }
        let sent = if app.flush {
            sink.send(chunk.clone()).await
        } else {
            sink.feed(chunk.clone()).await
        };
        if sent.is_err() {
            break;
        }
    }
}

/// A sender with a congestion window that grows like CUBIC and is cut by 30% when the buffer
/// of the link drops a packet: a stand-in for a competing download.
pub struct Competitor {
    /// Bytes acknowledged so far.
    pub delivered: Arc<AtomicU64>,
}

impl Competitor {
    /// Starts a flow on the direction at the given time since now.
    pub fn start(dir: &Arc<Dir>, after: Duration) -> Self {
        const MSS: f64 = 1448.0;
        const BETA: f64 = 0.7;
        const C: f64 = 0.4;
        let delivered = Arc::new(AtomicU64::new(0));
        let (dir, counter) = (dir.clone(), delivered.clone());
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let mut in_flight = 0.0_f64;
            // The window in packets; CUBIC after a loss: W(t) = C (t - K)^3 + W_max.
            let mut cwnd = 10.0_f64;
            let mut w_max = 10.0_f64;
            let mut epoch: Option<Instant> = None;
            let mut slow_start = true;
            let mut last_loss = Instant::now() - Duration::from_secs(60);
            let mut tick = tokio::time::interval(Duration::from_millis(1));
            loop {
                tick.tick().await;
                let acked = dir.take_foreign_acks() as f64;
                in_flight -= acked;
                counter.fetch_add(acked as u64, Ordering::Relaxed);
                let now = Instant::now();
                if slow_start {
                    cwnd += acked / MSS;
                } else if let Some(epoch) = epoch {
                    let t = (now - epoch).as_secs_f64();
                    let k = (w_max * (1.0 - BETA) / C).cbrt();
                    cwnd = (C * (t - k).powi(3) + w_max).max(2.0);
                }
                while in_flight + MSS <= cwnd * MSS {
                    if dir.push_foreign(MSS as usize) {
                        in_flight += MSS;
                        continue;
                    }
                    // A drop: one reduction per round trip is enough.
                    let rtt = Duration::from_millis(150);
                    if now - last_loss > rtt {
                        last_loss = now;
                        slow_start = false;
                        w_max = cwnd;
                        cwnd *= BETA;
                        epoch = Some(now);
                    }
                    break;
                }
            }
        });
        Self { delivered }
    }

    pub fn bytes(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }
}
