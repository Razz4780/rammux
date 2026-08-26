use std::{collections::VecDeque, num::NonZeroU32, ops::Not, time::Duration};

use tokio::time::Instant;

/// Global state shared by all rammux streams within a single rammux connection.
///
/// Used as the polling strategy of the connection's task selector.
#[derive(Default)]
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
    /// Sliding-window minimum of measured round-trip times.
    pub min_rtt: MinRtt,
    /// In-flight credit granted to us by the peer, if the transit window is enabled.
    pub transit_send: Option<TransitSend>,
    /// State of the transit window we grant to the peer, if enabled.
    pub transit_recv: Option<TransitRecv>,
}

impl GlobalPool {
    /// Records a fresh RTT measurement from a `PING` exchange.
    pub fn record_rtt(&mut self, rtt: Duration) {
        self.rtt = Some(rtt);
        self.min_rtt.push(rtt);
    }

    /// Notifies the transit window that `len` bytes of `DATA` payload were received
    /// and stored in this muxer.
    pub fn transit_recv_freed(&mut self, len: usize) {
        if let Some(recv) = self.transit_recv.as_mut() {
            let len = u32::try_from(len).unwrap_or(u32::MAX);
            recv.freed = recv.freed.saturating_add(len);
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
        let optimal = rtt
            .map(|rtt| Self::get_optimal(recv.freed, recv.last_update.elapsed(), rtt))
            .unwrap_or(recv.current);
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
    /// Time of the last produced window update.
    pub last_update: Instant,
}

impl TransitRecv {
    pub fn new(initial: NonZeroU32, max: u32, min_rtt_filter: bool) -> Self {
        Self {
            initial,
            current: initial.get(),
            freed: 0,
            max: max.max(initial.get()),
            min_rtt_filter,
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
