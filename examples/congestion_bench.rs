//! Research bench for rammux congestion behavior.
//!
//! Runs rammux (or a raw stream) over a real TCP connection or over a
//! userspace emulated link, and samples goodput, ping RTT, window state
//! and link queues to CSV on stdout.

use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures::{SinkExt, StreamExt};
use rammux::{
    RammuxError,
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::Instant,
};

#[derive(Parser, Debug)]
struct Args {
    /// rammux or raw byte stream.
    #[arg(long, value_enum, default_value_t = Mode::Rammux)]
    mode: Mode,
    /// Real TCP on loopback (shape externally) or userspace emulated link.
    #[arg(long, value_enum, default_value_t = TransportKind::Emu)]
    transport: TransportKind,

    // ---- emulated link parameters ----
    /// Link rate in Mbit/s.
    #[arg(long, default_value_t = 100.0)]
    rate_mbit: f64,
    /// Round-trip propagation delay in ms.
    #[arg(long, default_value_t = 20.0)]
    rtt_ms: f64,
    /// Random loss applied to the TCP model, percent.
    #[arg(long, default_value_t = 0.0)]
    loss_pct: f64,
    /// Mean duration of a loss burst in ms (0 = independent Bernoulli losses).
    #[arg(long, default_value_t = 0.0)]
    loss_burst_ms: f64,
    /// One-way delivery jitter bound in ms.
    #[arg(long, default_value_t = 0.0)]
    jitter_ms: f64,
    /// Emulated socket send buffer per direction, KiB.
    #[arg(long, default_value_t = 4096)]
    sndbuf_kb: u64,
    /// Both directions draw from one shared capacity pool (half-duplex media
    /// like Wi-Fi airtime) instead of having independent full-duplex capacity.
    #[arg(long, default_value_t = false)]
    shared_capacity: bool,
    /// In bi mode, delay the reverse (server-side) writers by this many seconds.
    #[arg(long, default_value_t = 0.0)]
    reverse_delay_s: f64,
    /// RNG seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    // ---- workload ----
    /// Concurrent rammux streams.
    #[arg(long, default_value_t = 8)]
    streams: usize,
    /// Bytes to send per stream; 0 means unbounded (run until --duration-s).
    #[arg(long, default_value_t = 0)]
    bytes_per_stream: u64,
    /// Hard stop for the run, seconds.
    #[arg(long, default_value_t = 60.0)]
    duration_s: f64,
    /// uni: client sends, server reads. bi: both directions.
    #[arg(long, value_enum, default_value_t = Direction::Uni)]
    direction: Direction,
    /// Application write chunk, KiB.
    #[arg(long, default_value_t = 64)]
    chunk_kb: usize,
    /// Artificial delay between reads on the receiving side, microseconds.
    #[arg(long, default_value_t = 0)]
    reader_delay_us: u64,

    // ---- rammux config ----
    #[arg(long, default_value_t = 5000)]
    ping_interval_ms: u64,
    #[arg(long, default_value_t = 4096)]
    global_window_kb: u32,
    #[arg(long, default_value_t = 64)]
    stream_window_kb: u32,
    #[arg(long, default_value_t = 16)]
    frame_limit_kb: u32,
    /// Transit (in-flight) window initial size, KiB. 0 disables it.
    #[arg(long, default_value_t = 0)]
    transit_kb: u32,
    /// Transit window autotune limit, KiB.
    #[arg(long, default_value_t = 4096)]
    transit_max_kb: u32,
    /// Use the min-RTT filter for transit window autotuning.
    #[arg(long, default_value_t = false)]
    transit_minrtt: bool,
    /// Gate transit window growth on arrival-rate increase.
    #[arg(long, default_value_t = false)]
    transit_bwgate: bool,

    // ---- sampling ----
    #[arg(long, default_value_t = 500)]
    sample_ms: u64,

    /// TCP congestion control for --transport tcp (must be in the allowed list).
    #[arg(long, default_value = "reno")]
    cc: String,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Mode {
    Rammux,
    Raw,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum TransportKind {
    Tcp,
    Emu,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq)]
enum Direction {
    Uni,
    Bi,
}

// ---------------------------------------------------------------------------
// Emulated link
// ---------------------------------------------------------------------------

const MSS: u64 = 1448;

struct Xorshift(u64);

impl Xorshift {
    fn next_f64(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Public gauges of one link direction.
#[derive(Default)]
struct DirGauges {
    /// Bytes accepted from the writer but not yet through the bottleneck.
    queued: AtomicU64,
    /// Model congestion window, bytes.
    cwnd: AtomicU64,
    /// Cumulative stall time from loss recovery, ms.
    stall_ms: AtomicU64,
    /// RTO events.
    rtos: AtomicU64,
}

struct LinkParams {
    rate_bps: f64,
    owd: Duration,
    loss: f64,
    /// Mean wall-clock duration of a loss burst.
    burst_mean: Duration,
    /// Loss probability inside a burst.
    burst_loss: f64,
    /// Probability of entering a burst per serviced MSS.
    burst_enter: f64,
    jitter: Duration,
    sndbuf: u64,
}

struct DirState {
    /// Chunks accepted from the writer, not yet serviced.
    pending: VecDeque<Bytes>,
    pending_bytes: u64,
    /// Serviced chunks waiting for their delivery instant.
    ready: VecDeque<(Instant, Bytes)>,
    /// When the bottleneck is free to service the next byte.
    free_at: Instant,
    /// Last delivery instant, to keep delivery in order under jitter.
    last_delivery: Instant,
    cwnd: f64,
    ssthresh: f64,
    /// Wall-clock end of the current loss burst, if inside one.
    burst_until: Option<Instant>,
    consecutive_rtos: u32,
    rng: Xorshift,
    closed: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

struct Dir {
    state: Mutex<DirState>,
    params: LinkParams,
    gauges: Arc<DirGauges>,
    kick: Notify,
    /// Busy-until clock of a shared half-duplex medium, if enabled.
    shared: Option<Arc<Mutex<Instant>>>,
}

impl Dir {
    fn new(params: LinkParams, seed: u64, shared: Option<Arc<Mutex<Instant>>>) -> Self {
        let now = Instant::now();
        Self {
            shared,
            gauges: Default::default(),
            state: Mutex::new(DirState {
                pending: Default::default(),
                pending_bytes: 0,
                ready: Default::default(),
                free_at: now,
                last_delivery: now,
                cwnd: (10 * MSS) as f64,
                ssthresh: f64::INFINITY,
                burst_until: None,
                consecutive_rtos: 0,
                rng: Xorshift(seed | 1),
                closed: false,
                read_waker: None,
                write_waker: None,
            }),
            params,
            kick: Notify::new(),
        }
    }

    /// Services queued bytes through the TCP-over-link model.
    ///
    /// Returns the next instant at which more progress can happen, if any.
    fn advance(&self, now: Instant) -> Option<Instant> {
        let mut st = self.state.lock().unwrap();
        let p = &self.params;
        let mut woke_reader = false;

        while !st.pending.is_empty() && st.free_at <= now {
            // A shared half-duplex medium must also be free; the quantum
            // starts when both the direction and the medium are available.
            let mut start = st.free_at;
            if let Some(shared) = &self.shared {
                let medium = *shared.lock().unwrap();
                if medium > now {
                    break;
                }
                start = start.max(medium);
            }
            // Service one MSS-sized quantum.
            let take = MSS.min(st.pending_bytes);
            let mut chunk = st.pending.pop_front().unwrap();
            let quantum = if (chunk.len() as u64) > take {
                let rest = chunk.split_off(take as usize);
                st.pending.push_front(rest);
                chunk
            } else {
                chunk
            };
            st.pending_bytes -= quantum.len() as u64;

            // Loss process (Gilbert-Elliott with wall-clock burst duration,
            // degenerates to Bernoulli when bursts are disabled).
            let roll = st.rng.next_f64();
            let in_burst = st.burst_until.is_some_and(|until| now < until);
            if !in_burst {
                st.burst_until = None;
                if st.rng.next_f64() < p.burst_enter {
                    let mean = p.burst_mean.as_secs_f64();
                    let duration = Duration::from_secs_f64(mean * (0.5 + st.rng.next_f64()));
                    st.burst_until = Some(now + duration);
                }
            }
            let loss_now = if in_burst {
                roll < p.burst_loss
            } else {
                roll < p.loss
            };

            // Congestion window reaction and head-of-line stall.
            let rtt = p.owd.as_secs_f64() * 2.0;
            let mut stall = Duration::ZERO;
            if loss_now {
                if st.cwnd <= (4 * MSS) as f64 {
                    // Retransmission timeout.
                    let rto = Duration::from_secs_f64(
                        (0.2f64 * (1 << st.consecutive_rtos.min(6)) as f64).max(2.0 * rtt),
                    );
                    st.consecutive_rtos += 1;
                    st.ssthresh = (st.cwnd / 2.0).max((2 * MSS) as f64);
                    st.cwnd = (2 * MSS) as f64;
                    stall = rto;
                    self.gauges.rtos.fetch_add(1, Ordering::Relaxed);
                } else {
                    // Fast recovery: one RTT of head-of-line blocking.
                    st.consecutive_rtos = 0;
                    st.ssthresh = (st.cwnd * 0.7).max((2 * MSS) as f64);
                    st.cwnd = st.ssthresh;
                    stall = Duration::from_secs_f64(rtt);
                }
                self.gauges
                    .stall_ms
                    .fetch_add(stall.as_millis() as u64, Ordering::Relaxed);
            } else {
                st.consecutive_rtos = 0;
                // Slow start / congestion avoidance growth.
                if st.cwnd < st.ssthresh {
                    st.cwnd += quantum.len() as f64;
                } else {
                    st.cwnd += (MSS * MSS) as f64 * quantum.len() as f64 / (st.cwnd * MSS as f64);
                }
                st.cwnd = st.cwnd.min(64.0 * 1024.0 * 1024.0);
            }

            // Service time: bottleneck rate, capped by window/RTT when RTT > 0.
            let window_rate = if rtt > 0.0 {
                st.cwnd / rtt
            } else {
                f64::INFINITY
            };
            let rate = p.rate_bps.min(window_rate).max(1.0);
            let service = Duration::from_secs_f64(quantum.len() as f64 / rate);
            st.free_at = start + service + stall;
            if let Some(shared) = &self.shared {
                // The medium is occupied for the wire time at full link rate;
                // window pacing and recovery stalls do not hold the medium.
                let mut medium = shared.lock().unwrap();
                let wire = Duration::from_secs_f64(quantum.len() as f64 / p.rate_bps);
                *medium = (*medium).max(start) + wire;
            }

            // Delivery: one-way delay plus jitter, in order.
            let jitter = Duration::from_secs_f64(p.jitter.as_secs_f64() * st.rng.next_f64());
            let deliver_at = (st.free_at + p.owd + jitter).max(st.last_delivery);
            st.last_delivery = deliver_at;
            st.ready.push_back((deliver_at, quantum));
            woke_reader = true;
        }

        self.gauges.queued.store(
            st.pending_bytes + st.ready.iter().map(|(_, b)| b.len() as u64).sum::<u64>(),
            Ordering::Relaxed,
        );
        self.gauges.cwnd.store(st.cwnd as u64, Ordering::Relaxed);

        if st.pending_bytes < p.sndbuf
            && let Some(w) = st.write_waker.take()
        {
            w.wake();
        }
        if woke_reader && let Some(w) = st.read_waker.take() {
            w.wake();
        }

        let next_ready = st.ready.front().map(|(at, _)| *at);
        let next_service = st.pending.front().map(|_| {
            let mut at = st.free_at;
            if let Some(shared) = &self.shared {
                at = at.max(*shared.lock().unwrap());
            }
            at
        });
        drop(st);
        [next_ready, next_service].into_iter().flatten().min()
    }

    async fn pump(self: Arc<Self>, stop: Arc<AtomicBool>) {
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let next = self.advance(Instant::now());
            // Wake pending readers whose delivery time has come.
            {
                let mut st = self.state.lock().unwrap();
                if st
                    .ready
                    .front()
                    .is_some_and(|(at, _)| *at <= Instant::now())
                    && let Some(w) = st.read_waker.take()
                {
                    w.wake();
                }
            }
            match next {
                Some(at) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(at) => {},
                        _ = self.kick.notified() => {},
                    }
                },
                None => self.kick.notified().await,
            }
        }
    }
}

/// One endpoint of the emulated link.
struct EmuStream {
    /// Direction this endpoint writes into.
    tx: Arc<Dir>,
    /// Direction this endpoint reads from.
    rx: Arc<Dir>,
    stop: Arc<AtomicBool>,
}

impl Drop for EmuStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.tx.kick.notify_waiters();
        self.rx.kick.notify_waiters();
    }
}

impl AsyncWrite for EmuStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut st = this.tx.state.lock().unwrap();
        if st.closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let free = this.tx.params.sndbuf.saturating_sub(st.pending_bytes);
        if free == 0 {
            st.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let take = (buf.len() as u64).min(free) as usize;
        if st.pending.is_empty() {
            // The bottleneck was idle - it cannot have accumulated credit.
            st.free_at = st.free_at.max(Instant::now());
        }
        st.pending.push_back(Bytes::copy_from_slice(&buf[..take]));
        st.pending_bytes += take as u64;
        drop(st);
        this.tx.kick.notify_waiters();
        Poll::Ready(Ok(take))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut st = this.tx.state.lock().unwrap();
        st.closed = true;
        drop(st);
        this.tx.kick.notify_waiters();
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for EmuStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let now = Instant::now();
        let mut st = this.rx.state.lock().unwrap();
        let mut filled_any = false;
        while buf.remaining() > 0 {
            match st.ready.front() {
                Some((at, chunk)) if *at <= now => {
                    let take = chunk.len().min(buf.remaining());
                    buf.put_slice(&chunk[..take]);
                    filled_any = true;
                    if take == chunk.len() {
                        st.ready.pop_front();
                    } else {
                        let (at, mut chunk) = st.ready.pop_front().unwrap();
                        let rest = chunk.split_off(take);
                        st.ready.push_front((at, rest));
                        let _ = chunk;
                    }
                },
                _ => break,
            }
        }
        if filled_any {
            drop(st);
            this.rx.kick.notify_waiters();
            return Poll::Ready(Ok(()));
        }
        if st.closed && st.pending.is_empty() && st.ready.is_empty() {
            return Poll::Ready(Ok(()));
        }
        st.read_waker = Some(cx.waker().clone());
        drop(st);
        this.rx.kick.notify_waiters();
        Poll::Pending
    }
}

fn emu_pair(args: &Args) -> (EmuStream, EmuStream, Arc<DirGauges>, Arc<DirGauges>) {
    let params = || {
        let loss = args.loss_pct / 100.0;
        // Gilbert-Elliott derivation: bad-state loss fixed at 50%, sojourn
        // times derived from mean burst length and long-run loss average.
        let (burst_enter, burst_mean, burst_loss, bernoulli) = if args.loss_burst_ms > 0.0 {
            let burst_loss = 0.33f64;
            // Long-run loss average: fraction of time in bursts times burst_loss.
            // Quanta per second at full rate determines the per-quantum enter probability
            // that yields the requested average.
            let quanta_per_s = args.rate_mbit * 1e6 / 8.0 / MSS as f64;
            let bursts_per_s = loss / burst_loss / (args.loss_burst_ms / 1000.0);
            let enter = (bursts_per_s / quanta_per_s).clamp(0.0, 1.0);
            (
                enter,
                Duration::from_secs_f64(args.loss_burst_ms / 1000.0),
                burst_loss,
                0.0,
            )
        } else {
            (0.0, Duration::ZERO, 0.0, loss)
        };
        LinkParams {
            rate_bps: args.rate_mbit * 1e6 / 8.0,
            owd: Duration::from_secs_f64(args.rtt_ms / 2000.0),
            loss: bernoulli,
            burst_enter,
            burst_mean,
            burst_loss,
            jitter: Duration::from_secs_f64(args.jitter_ms / 1000.0),
            sndbuf: args.sndbuf_kb * 1024,
        }
    };
    let shared = args
        .shared_capacity
        .then(|| Arc::new(Mutex::new(Instant::now())));
    let ab = Arc::new(Dir::new(
        params(),
        args.seed.wrapping_mul(0x9E3779B97F4A7C15),
        shared.clone(),
    ));
    let ba = Arc::new(Dir::new(
        params(),
        args.seed.wrapping_mul(0xD1B54A32D192ED03),
        shared,
    ));
    let stop = Arc::new(AtomicBool::new(false));
    tokio::spawn(ab.clone().pump(stop.clone()));
    tokio::spawn(ba.clone().pump(stop.clone()));
    let g_ab = ab.gauges.clone();
    let g_ba = ba.gauges.clone();
    (
        EmuStream {
            tx: ab.clone(),
            rx: ba.clone(),
            stop: stop.clone(),
        },
        EmuStream {
            tx: ba,
            rx: ab,
            stop,
        },
        g_ab,
        g_ba,
    )
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

fn rammux_config(args: &Args) -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.frame_limit = (args.frame_limit_kb * 1024).try_into().unwrap();
    config.local_recv_window = (args.stream_window_kb * 1024).try_into().unwrap();
    config.remote_recv_window = args.stream_window_kb * 1024;
    config.ping_interval = Duration::from_millis(args.ping_interval_ms);
    config.global_recv_window = args.global_window_kb as usize * 1024;
    config.local_transit_window = args.transit_kb * 1024;
    config.remote_transit_window = args.transit_kb * 1024;
    config.transit_window_max = args.transit_max_kb * 1024;
    config.transit_min_rtt_filter = args.transit_minrtt;
    config.transit_bw_gate = args.transit_bwgate;
    config
}

struct Counters {
    delivered_to_client: AtomicU64,
    delivered_to_server: AtomicU64,
}

async fn drive_conn<IO>(
    mut conn: RammuxConnection<IO>,
    args: &Args,
    counters: Arc<Counters>,
    started: Instant,
) -> Result<(), RammuxError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let role = conn.role();
    let is_client = role == RammuxRole::Client;
    let mut to_start = if is_client { args.streams } else { 0 };

    let mut sample = tokio::time::interval(Duration::from_millis(args.sample_ms));
    sample.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        while to_start > 0 {
            match conn.try_start_outbound() {
                Ok(Some(duplex)) => {
                    to_start -= 1;
                    let write = true;
                    let read = args.direction == Direction::Bi;
                    spawn_stream(
                        duplex,
                        args,
                        write,
                        read,
                        counters.clone(),
                        is_client,
                        Duration::ZERO,
                    );
                },
                Ok(None) => break,
                Err(error) => return Err(error),
            }
        }

        tokio::select! {
            progress = conn.progress() => match progress? {
                RammuxProgress::Inbound(duplex) => {
                    let write = args.direction == Direction::Bi;
                    let read = true;
                    spawn_stream(
                        duplex,
                        args,
                        write,
                        read,
                        counters.clone(),
                        is_client,
                        Duration::from_secs_f64(args.reverse_delay_s),
                    );
                },
                RammuxProgress::Downgraded(..) => return Ok(()),
                RammuxProgress::Empty => {},
            },
            _ = sample.tick() => {
                let stats = conn.stats();
                let rtt_ms = stats.rtt.map(|d| d.as_secs_f64() * 1e3).unwrap_or(-1.0);
                println!(
                    "{:.3},{role},stats,rtt_ms={rtt_ms:.2},pool_kb={},in={},out={},tc_kb={},tw_kb={}",
                    started.elapsed().as_secs_f64(),
                    stats.available_global_recv_window / 1024,
                    stats.inbound_streams,
                    stats.outbound_streams,
                    stats.transit_send_credit.map(|c| (c / 1024) as i64).unwrap_or(-1),
                    stats.transit_recv_window.map(|w| (w / 1024) as i64).unwrap_or(-1),
                );
            }
        }
    }
}

fn spawn_stream(
    duplex: rammux::stream::RammuxDuplex,
    args: &Args,
    write: bool,
    read: bool,
    counters: Arc<Counters>,
    on_client: bool,
    write_delay: Duration,
) {
    let chunk = Bytes::from(vec![0u8; args.chunk_kb * 1024]);
    let target = args.bytes_per_stream;
    let reader_delay = Duration::from_micros(args.reader_delay_us);
    let (mut sink, mut stream) = duplex.into_split();
    tokio::spawn(async move {
        let writer = async {
            if write {
                if !write_delay.is_zero() {
                    tokio::time::sleep(write_delay).await;
                }
                let mut sent = 0u64;
                while target == 0 || sent < target {
                    let chunk = if target > 0 && target - sent < chunk.len() as u64 {
                        chunk.clone().split_to((target - sent) as usize)
                    } else {
                        chunk.clone()
                    };
                    sent += chunk.len() as u64;
                    if sink.feed(chunk).await.is_err() {
                        return;
                    }
                }
                let _ = sink.close().await;
            } else {
                let _ = sink.close().await;
            }
        };
        let reader = async {
            if read {
                let counter = if on_client {
                    &counters.delivered_to_client
                } else {
                    &counters.delivered_to_server
                };
                while let Some(data) = stream.next().await {
                    counter.fetch_add(data.len() as u64, Ordering::Relaxed);
                    if !reader_delay.is_zero() {
                        tokio::time::sleep(reader_delay).await;
                    }
                }
            }
        };
        tokio::join!(writer, reader);
    });
}

async fn run_rammux<IO>(io_client: IO, io_server: IO, args: &Args, counters: Arc<Counters>)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let started = Instant::now();
    let client = RammuxConnection::new(
        RammuxRole::Client,
        io_client,
        rammux_config(args),
    );
    let server = RammuxConnection::new(
        RammuxRole::Server,
        io_server,
        rammux_config(args),
    );

    let client_task = drive_conn(client, args, counters.clone(), started);
    let server_task = drive_conn(server, args, counters.clone(), started);
    let deadline = tokio::time::sleep(Duration::from_secs_f64(args.duration_s));

    tokio::select! {
        results = futures::future::join(client_task, server_task) => {
            match results {
                (Ok(()), Ok(())) => println!("{:.3},both,done,", started.elapsed().as_secs_f64()),
                (client, server) => {
                    if let Err(error) = client {
                        println!("{:.3},client,failed,{error}", started.elapsed().as_secs_f64());
                    }
                    if let Err(error) = server {
                        println!("{:.3},server,failed,{error}", started.elapsed().as_secs_f64());
                    }
                },
            }
        },
        _ = deadline => {
            println!("{:.3},both,deadline,", started.elapsed().as_secs_f64());
        }
    }
}

async fn run_raw<IO>(io_client: IO, io_server: IO, args: &Args, counters: Arc<Counters>)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let started = Instant::now();
    let chunk = Bytes::from(vec![0u8; args.chunk_kb * 1024]);
    let bi = args.direction == Direction::Bi;

    let (mut cr, mut cw) = tokio::io::split(io_client);
    let (mut sr, mut sw) = tokio::io::split(io_server);

    let counters_2 = counters.clone();
    let chunk_2 = chunk.clone();
    let client_writer = tokio::spawn(async move {
        loop {
            if cw.write_all(&chunk_2).await.is_err() {
                return;
            }
        }
    });
    let server_reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            match sr.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    counters_2
                        .delivered_to_server
                        .fetch_add(n as u64, Ordering::Relaxed);
                },
            }
        }
    });
    let counters_3 = counters.clone();
    let server_writer = tokio::spawn(async move {
        if !bi {
            return;
        }
        loop {
            if sw.write_all(&chunk).await.is_err() {
                return;
            }
        }
    });
    let client_reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            match cr.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    counters_3
                        .delivered_to_client
                        .fetch_add(n as u64, Ordering::Relaxed);
                },
            }
        }
    });

    tokio::time::sleep(Duration::from_secs_f64(args.duration_s)).await;
    println!("{:.3},both,deadline,", started.elapsed().as_secs_f64());
    client_writer.abort();
    server_reader.abort();
    server_writer.abort();
    client_reader.abort();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("# {args:?}");
    println!("# csv: t_s,side,event,details");

    let counters = Arc::new(Counters {
        delivered_to_client: AtomicU64::new(0),
        delivered_to_server: AtomicU64::new(0),
    });

    // Goodput + link gauge sampler.
    let sampler_counters = counters.clone();
    let sample_ms = args.sample_ms;
    let spawn_sampler = |gauges: Option<(Arc<DirGauges>, Arc<DirGauges>)>| {
        tokio::spawn(async move {
            let started = Instant::now();
            let mut last_c = 0u64;
            let mut last_s = 0u64;
            let mut interval = tokio::time::interval(Duration::from_millis(sample_ms));
            loop {
                interval.tick().await;
                let c = sampler_counters.delivered_to_client.load(Ordering::Relaxed);
                let s = sampler_counters.delivered_to_server.load(Ordering::Relaxed);
                let dt = sample_ms as f64 / 1000.0;
                let to_client_mbps = (c - last_c) as f64 * 8.0 / 1e6 / dt;
                let to_server_mbps = (s - last_s) as f64 * 8.0 / 1e6 / dt;
                last_c = c;
                last_s = s;
                let link = gauges
                    .as_ref()
                    .map(|(ab, ba)| {
                        format!(
                            "q_ab_kb={},q_ba_kb={},cwnd_ab_kb={},cwnd_ba_kb={},stall_ab_ms={},stall_ba_ms={},rtos_ab={},rtos_ba={}",
                            ab.queued.load(Ordering::Relaxed) / 1024,
                            ba.queued.load(Ordering::Relaxed) / 1024,
                            ab.cwnd.load(Ordering::Relaxed) / 1024,
                            ba.cwnd.load(Ordering::Relaxed) / 1024,
                            ab.stall_ms.load(Ordering::Relaxed),
                            ba.stall_ms.load(Ordering::Relaxed),
                            ab.rtos.load(Ordering::Relaxed),
                            ba.rtos.load(Ordering::Relaxed),
                        )
                    })
                    .unwrap_or_default();
                println!(
                    "{:.3},link,goodput,to_client_mbps={to_client_mbps:.2},to_server_mbps={to_server_mbps:.2},{link}",
                    started.elapsed().as_secs_f64(),
                );
            }
        })
    };

    match args.transport {
        TransportKind::Emu => {
            let (io_client, io_server, g_ab, g_ba) = emu_pair(&args);
            spawn_sampler(Some((g_ab, g_ba)));
            match args.mode {
                Mode::Rammux => run_rammux(io_client, io_server, &args, counters.clone()).await,
                Mode::Raw => run_raw(io_client, io_server, &args, counters.clone()).await,
            }
        },
        TransportKind::Tcp => {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let io_client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let io_server = listener.accept().await.unwrap().0;
            for stream in [&io_client, &io_server] {
                stream.set_nodelay(true).unwrap();
                let sock = socket2::SockRef::from(stream);
                sock.set_tcp_congestion(args.cc.as_bytes())
                    .expect("failed to set congestion control");
            }
            spawn_sampler(None);
            match args.mode {
                Mode::Rammux => run_rammux(io_client, io_server, &args, counters.clone()).await,
                Mode::Raw => run_raw(io_client, io_server, &args, counters.clone()).await,
            }
        },
    }
}
