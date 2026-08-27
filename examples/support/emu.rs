//! Userspace link emulator shared by the bench examples.
//!
//! Models a TCP-like delivery process over a configurable link: token-bucket
//! bottleneck (optionally shared between directions), propagation delay,
//! finite send buffer, Reno-style AIMD with fast-recovery stalls and RTO
//! backoff, and wall-clock Gilbert-Elliott loss bursts.
#![allow(dead_code)]

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
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
    time::Instant,
};

// ---------------------------------------------------------------------------
// Emulated link
// ---------------------------------------------------------------------------

pub const MSS: u64 = 1448;

pub struct Xorshift(u64);

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
pub struct DirGauges {
    /// Bytes accepted from the writer but not yet through the bottleneck.
    pub queued: AtomicU64,
    /// Model congestion window, bytes.
    pub cwnd: AtomicU64,
    /// Cumulative stall time from loss recovery, ms.
    pub stall_ms: AtomicU64,
    /// RTO events.
    pub rtos: AtomicU64,
}

pub struct LinkParams {
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

pub struct DirState {
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

pub struct Dir {
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
pub struct EmuStream {
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

/// Link parameters for building an emulated pair.
#[derive(Clone, Copy, Debug)]
pub struct EmuOpts {
    pub rate_mbit: f64,
    pub rtt_ms: f64,
    pub loss_pct: f64,
    pub loss_burst_ms: f64,
    pub jitter_ms: f64,
    pub sndbuf_kb: u64,
    pub shared_capacity: bool,
    pub seed: u64,
}

pub fn emu_pair(args: &EmuOpts) -> (EmuStream, EmuStream, Arc<DirGauges>, Arc<DirGauges>) {
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
