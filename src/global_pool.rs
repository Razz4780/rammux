use std::{collections::VecDeque, num::NonZeroU32, ops::Not, time::Duration};

use tokio::time::Instant;

/// Global state shared by all rammux streams within a single rammux connection.
///
/// Used as the polling strategy of the connection's task selector.
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
    /// Sliding-window minimum of measured round-trip times.
    pub min_rtt: MinRtt,
    /// State of the cooperative link-clearing probe.
    pub probe: ProbeState,
    /// Latest RTT measured over a cooperatively cleared link
    /// (or, before the first probe, the connection's first ping).
    pub clean_rtt: Option<Duration>,
    /// When the last probe finished, for scheduling.
    pub last_probe: Option<Instant>,
    /// A `CLEAR_LINK` frame should be sent as soon as possible.
    pub probe_send_clear: bool,
    /// Our next `PING` should be sent as soon as possible, and its
    /// response is a clean sample.
    pub probe_force_ping: bool,
    /// The response to the ping currently in flight is a clean sample.
    pub probe_mark_clean: bool,
    /// In-flight credit granted to us by the peer, if the transit window is enabled.
    pub transit_send: Option<TransitSend>,
    /// State of the transit window we grant to the peer, if enabled.
    pub transit_recv: Option<TransitRecv>,
    /// Use the clean-probe RTT for every window-sizing decision
    /// (stream autotune and the transit rate meter), not just the
    /// transit growth policy.
    pub clean_rtt_sizing: bool,
    /// Per-stream window autotune gain: the window targets
    /// `gain x rate x RTT`.
    pub stream_window_gain: f64,
    /// Per-stream window growth limit: the window can at most multiply
    /// by this factor in a single update round.
    pub stream_window_growth: u32,
}

impl Default for GlobalPool {
    fn default() -> Self {
        Self {
            rtt: None,
            available: 0,
            min_rtt: Default::default(),
            probe: Default::default(),
            clean_rtt: None,
            last_probe: None,
            probe_send_clear: false,
            probe_force_ping: false,
            probe_mark_clean: false,
            transit_send: None,
            transit_recv: None,
            clean_rtt_sizing: false,
            stream_window_gain: 1.5,
            stream_window_growth: 2,
        }
    }
}

impl GlobalPool {
    /// Records a fresh RTT measurement from a `PING` exchange.
    pub fn record_rtt(&mut self, rtt: Duration) {
        if self.rtt.is_none() {
            // The connection starts with a ping on an empty link -
            // the first sample is clean by construction.
            self.clean_rtt = Some(rtt);
        }
        self.rtt = Some(rtt);
        self.min_rtt.push(rtt);
        if self.probe_mark_clean {
            self.probe_mark_clean = false;
            self.clean_rtt = Some(rtt);
            match self.probe {
                ProbeState::InitPing { .. } | ProbeState::RespFinish => {
                    self.probe = ProbeState::Idle;
                    self.last_probe = Some(Instant::now());
                },
                _ => {},
            }
        }
    }

    /// The RTT yardstick for window sizing.
    ///
    /// With [`Self::clean_rtt_sizing`] and a running link-clearing probe
    /// this is the clean-probe sample - a value the connection's own
    /// standing queue cannot inflate - falling back to the raw sample
    /// until the first ping completes. Without the probe there is nothing
    /// keeping clean samples fresh, so sizing uses the latest raw sample,
    /// queueing included.
    pub fn sizing_rtt(&self) -> Option<Duration> {
        let probing = self
            .transit_recv
            .as_ref()
            .is_some_and(|recv| recv.clean_policy);
        if self.clean_rtt_sizing && probing {
            self.clean_rtt.or(self.rtt)
        } else {
            self.rtt
        }
    }

    /// Returns whether outbound frames other than probe/control frames
    /// must be held back for a link-clearing probe.
    pub fn probe_paused(&self) -> bool {
        matches!(
            self.probe,
            ProbeState::InitWait { .. } | ProbeState::InitPing { .. } | ProbeState::RespWait { .. }
        )
    }

    /// Notifies the transit window that `len` bytes of `DATA` payload were received
    /// and stored in this muxer.
    pub fn transit_recv_freed(&mut self, len: usize) {
        let rtt = self.sizing_rtt();
        if let Some(recv) = self.transit_recv.as_mut() {
            let len = u32::try_from(len).unwrap_or(u32::MAX);
            recv.freed = recv.freed.saturating_add(len);
            if recv.bw_gate || recv.clean_policy {
                // Measure the arrival rate over buckets of at least
                // max(2 x RTT, 10ms) - rates sampled over shorter horizons
                // are burst artifacts, not throughput.
                recv.rate_bucket_bytes += u64::from(len);
                let bucket =
                    (2 * rtt.unwrap_or(Duration::from_millis(5))).max(Duration::from_millis(10));
                let elapsed = recv.rate_bucket_start.elapsed();
                if elapsed >= bucket {
                    let rate = recv.rate_bucket_bytes as f64 / elapsed.as_secs_f64();
                    recv.rate_ema = if recv.rate_ema == 0.0 {
                        rate
                    } else {
                        0.5 * recv.rate_ema + 0.5 * rate
                    };
                    recv.rate_bucket_bytes = 0;
                    recv.rate_bucket_start = Instant::now();
                }
            }
        }
    }

    /// Produces the next `SESSION_WINDOW_UPDATE` value, if one is due.
    pub fn transit_recv_update(&mut self) -> Option<u32> {
        let recv = self.transit_recv.as_mut()?;
        if recv.can_update().not() {
            return None;
        }

        let rtt = if recv.min_rtt_filter {
            self.min_rtt.get().or(self.rtt)
        } else {
            self.rtt
        };
        let elapsed = recv.last_update.elapsed();
        let optimal = if recv.clean_policy {
            // Growth needs evidence of window limitation, judged against the
            // clean-probe RTT - a yardstick our own queue cannot inflate.
            // The measured arrival rate is capped by the current window, so
            // it cannot size the next window directly (chicken-and-egg);
            // instead it bounds growth: the window doubles while limited,
            // ceilinged at 2 x rate x clean RTT, which stops growth at
            // about twice the loop BDP once the link is the bottleneck.
            // Grow-only for now.
            match self.clean_rtt {
                Some(clean) if recv.rate_ema > 0.0 && elapsed < 2 * clean => {
                    #[allow(clippy::cast_possible_truncation)]
                    let ceiling = (2.0 * clean.as_secs_f64() * recv.rate_ema) as u32;
                    recv.current
                        .saturating_mul(2)
                        .min(ceiling)
                        .max(recv.current)
                },
                _ => recv.current,
            }
        } else if recv.bw_gate {
            // Growth trigger: evidence of window limitation - the window is
            // cycling faster than the credit loop can breathe. Deliberately
            // uses the latest RTT sample (the operational loop time, queueing
            // included), like quiche's 2x smoothed-RTT rule: permissiveness
            // here is safe because growth is still gated on the arrival rate.
            // Growth gate: the bucket-measured, direction-pure arrival rate
            // must demonstrably increase - under saturation it caps at the
            // link share, so self-inflicted queueing can never feed growth.
            let window_limited = self.rtt.is_some_and(|rtt| elapsed < 2 * rtt);
            // The 1.25 margin keeps measurement noise on a rate plateau from
            // producing record samples that would each double the window
            // (compare BBR's 1.25x bandwidth probe): growth must demonstrably
            // pay for itself.
            if window_limited && recv.rate_ema > 0.0 && recv.rate_ema > recv.max_rate * 1.25 {
                recv.max_rate = recv.rate_ema;
                recv.current.saturating_mul(2)
            } else {
                // Deliberately asymmetric, like every battle-tested
                // implementation: the autotuner never shrinks. Shrinking
                // belongs to an external signal (memory pressure, idle).
                recv.current
            }
        } else {
            rtt.map(|rtt| Self::get_optimal(recv.freed, elapsed, rtt))
                .unwrap_or(recv.current)
        };
        let clamped = optimal
            // Window cannot shrink below the initial size.
            .max(recv.initial.get())
            // Window cannot shrink by more than 25% in one round.
            .max(recv.current - recv.current / 4)
            // Window cannot grow by more than 100% in one round.
            .min(recv.current.saturating_mul(2))
            // Window cannot outgrow the configured limit.
            .min(recv.max);

        let update = if clamped > recv.current {
            recv.freed + (clamped - recv.current)
        } else {
            recv.freed - (recv.current - clamped)
        };
        recv.current = clamped;
        recv.freed = 0;
        recv.last_update = Instant::now();

        (update > 0).then_some(update)
    }

    /// Same policy as the per-stream receive window autotune.
    #[allow(clippy::cast_possible_truncation)]
    fn get_optimal(freed: u32, time_since_update: Duration, rtt: Duration) -> u32 {
        let new_window =
            1.5 * rtt.as_secs_f64() * f64::from(freed) / time_since_update.as_secs_f64();
        new_window as u32
    }
}

/// State machine of the cooperative link-clearing probe (`CLEAR_LINK`).
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProbeState {
    /// No probe in progress.
    #[default]
    Idle,
    /// We initiated: data paused, `CLEAR_LINK` sent, waiting for the peer's.
    InitWait { since: Instant },
    /// Peer's `CLEAR_LINK` received: data paused, our probe ping in flight.
    InitPing { since: Instant },
    /// Peer initiated: data paused, our `CLEAR_LINK` queued,
    /// waiting for the peer's probe ping request.
    RespWait { since: Instant },
    /// Pong and own probe ping queued, data resumed,
    /// waiting for our response to record the clean sample.
    RespFinish,
}

/// Sender side of the transit window: how many more bytes of `DATA` payload
/// we may put in flight towards the peer.
pub struct TransitSend {
    pub credit: u32,
}

/// Receiver side of the transit window: tracks credit to return to the peer.
///
/// Credit is freed as soon as `DATA` payload is received and stored in this muxer,
/// independently of when the application reads it.
pub struct TransitRecv {
    /// Initial window size, also the shrink floor.
    pub initial: NonZeroU32,
    /// Current window size.
    pub current: u32,
    /// Bytes freed since the last update.
    pub freed: u32,
    /// Autotune growth limit.
    pub max: u32,
    /// Use the sliding-window minimum RTT instead of the latest sample.
    pub min_rtt_filter: bool,
    /// Gate growth on the payload arrival rate exceeding the best rate seen so far.
    pub bw_gate: bool,
    /// Size the window from the clean-probe RTT and the arrival rate; grow-only.
    pub clean_policy: bool,
    /// Smoothed arrival rate, bytes per second.
    pub rate_ema: f64,
    /// Best smoothed arrival rate observed, bytes per second.
    pub max_rate: f64,
    /// Start of the current rate-measurement bucket.
    pub rate_bucket_start: Instant,
    /// Payload bytes received in the current rate-measurement bucket.
    pub rate_bucket_bytes: u64,
    /// Time of the last produced window update.
    pub last_update: Instant,
}

impl TransitRecv {
    pub fn new(initial: NonZeroU32, max: u32, min_rtt_filter: bool, bw_gate: bool) -> Self {
        Self {
            initial,
            current: initial.get(),
            freed: 0,
            max: max.max(initial.get()),
            min_rtt_filter,
            bw_gate,
            clean_policy: false,
            rate_ema: 0.0,
            max_rate: 0.0,
            rate_bucket_start: Instant::now(),
            rate_bucket_bytes: 0,
            last_update: Instant::now(),
        }
    }

    fn can_update(&self) -> bool {
        self.freed >= self.current / 2
    }
}

/// Sliding-window minimum RTT estimator.
///
/// Keeps a monotonic deque of samples from the last [`Self::WINDOW`].
#[derive(Default)]
pub struct MinRtt {
    samples: VecDeque<(Instant, Duration)>,
}

impl MinRtt {
    const WINDOW: Duration = Duration::from_secs(600);

    pub fn push(&mut self, rtt: Duration) {
        let now = Instant::now();
        while self.samples.back().is_some_and(|(_, d)| *d >= rtt) {
            self.samples.pop_back();
        }
        self.samples.push_back((now, rtt));
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > Self::WINDOW)
        {
            self.samples.pop_front();
        }
    }

    pub fn get(&self) -> Option<Duration> {
        self.samples.front().map(|(_, d)| *d)
    }
}
