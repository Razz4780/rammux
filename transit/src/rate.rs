//! Smoothed throughput estimate.
//!
//! Two rules size a window from an arrival rate, and what they want is the rate
//! the *path* carries. The rate over the last interval is not that: it is
//! whatever the peer's scheduling and the application's write cadence made it.
//!
//! Worth knowing before reaching for this: smoothing has never moved a number
//! here. It is kept because the raw per-interval rate measures the caller's
//! cadence rather than the link, not because it improved a result. And a mean
//! is arguably the wrong statistic outright - the error is one-sided, since a
//! window-limited connection can only ever *under*estimate the bottleneck, so a
//! maximum over a sliding window (as BBR does) would be the better estimator.
//! [`crate::window::Growth::Ledbat`] sidesteps the question by estimating no
//! bandwidth at all.

use std::time::Duration;

use tokio::time::Instant;

/// How much round trip the estimate remembers.
///
/// Counted in round trips rather than seconds because the loop being sized is a
/// round-trip loop: four averages over several window updates while still
/// following a real change in the path within a few of them.
const TAU_RTTS: u32 = 4;

/// Floor on that memory, for paths whose round trip smooths nothing.
const TAU_FLOOR: Duration = Duration::from_millis(20);

/// The shortest interval worth dividing by.
///
/// Frames arriving microseconds apart would each report an instantaneous rate
/// of hundreds of gigabits, and two landing on the same instant would divide
/// by zero. The time correction already weights a short interval down, so this
/// only has to keep the divisor away from zero.
const BUCKET: Duration = Duration::from_millis(1);

/// The time constant to smooth over, for a path with this round trip.
pub(crate) fn tau(rtt: Option<Duration>) -> Duration {
    rtt.map_or(TAU_FLOOR, |rtt| (TAU_RTTS * rtt).max(TAU_FLOOR))
}

/// An exponentially weighted moving average of a throughput, in bytes per
/// second.
///
/// The weight comes from elapsed time rather than from a sample count:
/// `alpha = 1 - exp(-dt / tau)`. Samples arrive whenever data does, so a fixed
/// per-sample weight would give a memory that shrinks as the link gets faster,
/// which is exactly when the estimate is fed most often.
#[derive(Debug)]
pub(crate) struct RateEstimate {
    /// Current estimate. Zero until the first bucket closes.
    value: f64,
    /// Bytes seen since the open bucket started.
    pending: u64,
    /// When it started.
    since: Instant,
}

impl Default for RateEstimate {
    fn default() -> Self {
        Self {
            value: 0.0,
            pending: 0,
            since: Instant::now(),
        }
    }
}

impl RateEstimate {
    /// Folds `bytes` into the estimate.
    pub(crate) fn observe(&mut self, bytes: u64, tau: Duration) {
        self.pending += bytes;
        let elapsed = self.since.elapsed();
        if elapsed < BUCKET {
            return;
        }
        let sample = self.pending as f64 / elapsed.as_secs_f64();
        self.value = if self.value == 0.0 {
            sample
        } else {
            let alpha = 1.0 - (-elapsed.as_secs_f64() / tau.as_secs_f64()).exp();
            self.value + alpha * (sample - self.value)
        };
        self.pending = 0;
        self.since = Instant::now();
    }

    /// Bytes per second, zero before the first bucket closes.
    pub(crate) fn get(&self) -> f64 {
        self.value
    }
}
