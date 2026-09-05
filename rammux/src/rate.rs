//! Smoothed throughput estimates.
//!
//! Both window autotunes size themselves from a rate: the transit window from
//! what is arriving on the connection, a stream's receive window from what its
//! reader is draining. Neither wants the rate over the last interval, which is
//! whatever the transport's burstiness, the peer's scheduling and the
//! application's read cadence made it - they want the rate the path is
//! actually carrying.

use std::time::Duration;

use tokio::time::Instant;

/// How much round trip the estimate remembers.
///
/// The loop being sized is a round-trip loop, so the memory is expressed in
/// round trips rather than seconds: four is long enough to average over
/// several window updates and short enough to follow a real change in the
/// path within a few of them.
pub const RATE_TAU_RTTS: u32 = 4;

/// Floor on that memory.
///
/// On a sub-millisecond path four round trips is a few hundred microseconds,
/// which would smooth nothing. Twenty milliseconds still holds tens of
/// samples at any rate worth estimating.
pub const RATE_TAU_FLOOR: Duration = Duration::from_millis(20);

/// The time constant to smooth over, for a path with this round trip.
pub fn rate_tau(rtt: Option<Duration>) -> Duration {
    rtt.map_or(RATE_TAU_FLOOR, |rtt| {
        (RATE_TAU_RTTS * rtt).max(RATE_TAU_FLOOR)
    })
}

/// An exponentially weighted moving average of a throughput, in bytes per
/// second.
///
/// The weight is taken from elapsed time rather than from a sample count:
/// `alpha = 1 - exp(-dt / tau)`. Samples arrive whenever data does, so a
/// fixed per-sample weight would give a memory that shrinks as the link gets
/// faster - exactly when the estimate is fed most often. With the time
/// correction the memory is `tau` however the samples fall, and a caller may
/// observe every frame or every window update without changing what the
/// estimate means.
#[derive(Debug)]
pub struct RateEstimate {
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
    ///
    /// Bytes accumulate until enough time has passed to divide by: frames
    /// arriving microseconds apart would otherwise each produce an
    /// instantaneous rate of hundreds of gigabits, and while the time
    /// correction weights those to nothing, the arithmetic to get there
    /// runs through an infinity when two land on the same instant.
    pub fn observe(&mut self, bytes: u64, tau: Duration) {
        self.pending += bytes;
        let elapsed = self.since.elapsed();
        if elapsed < Self::bucket() {
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

    /// The current estimate, in bytes per second. Zero before the first
    /// bucket closes.
    pub fn get(&self) -> f64 {
        self.value
    }

    /// The shortest interval worth dividing by.
    ///
    /// Flat rather than a share of `tau`: the time correction already weights
    /// a short interval down in proportion, so the only job left here is to
    /// keep the divisor away from zero. Scaling it with `tau` would also
    /// delay the first estimate by an eighth of it, which on a long path is
    /// long enough for a short stream to finish without ever sizing itself.
    fn bucket() -> Duration {
        Duration::from_millis(1)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const TAU: Duration = Duration::from_millis(80);

    /// Feeds `rate` bytes per second for `total`, in slices of `step`.
    async fn feed(estimate: &mut RateEstimate, rate: f64, step: Duration, total: Duration) {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let steps = (total.as_secs_f64() / step.as_secs_f64()) as u32;
        for _ in 0..steps {
            tokio::time::advance(step).await;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            estimate.observe((rate * step.as_secs_f64()) as u64, TAU);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn it_converges_to_a_steady_rate() {
        let mut estimate = RateEstimate::default();
        feed(&mut estimate, 1e6, Duration::from_millis(5), 10 * TAU).await;
        let error = (estimate.get() - 1e6).abs() / 1e6;
        assert!(
            error < 0.02,
            "{} is not within 2% of 1 MB/s",
            estimate.get()
        );
    }

    /// The point of weighting by elapsed time. The same throughput delivered
    /// in frame-sized dribbles and in window-sized lumps has to land on the
    /// same number, or the estimate measures the caller's cadence rather than
    /// the path.
    #[tokio::test(start_paused = true)]
    async fn the_sample_cadence_does_not_change_the_answer() {
        let mut fine = RateEstimate::default();
        feed(&mut fine, 1e6, Duration::from_millis(2), 10 * TAU).await;
        let mut coarse = RateEstimate::default();
        feed(&mut coarse, 1e6, Duration::from_millis(40), 10 * TAU).await;
        let spread = (fine.get() - coarse.get()).abs() / fine.get();
        assert!(
            spread < 0.05,
            "{} vs {} differ by more than 5%",
            fine.get(),
            coarse.get(),
        );
    }

    /// A rate that steps has to be followed, and within a few time constants:
    /// an estimate that lags a real change in the path sizes the window for a
    /// link that is no longer there.
    #[tokio::test(start_paused = true)]
    async fn it_follows_a_step() {
        let mut estimate = RateEstimate::default();
        feed(&mut estimate, 1e6, Duration::from_millis(5), 10 * TAU).await;
        feed(&mut estimate, 4e6, Duration::from_millis(5), 3 * TAU).await;
        let error = (estimate.get() - 4e6).abs() / 4e6;
        assert!(
            error < 0.1,
            "{} has not reached 4 MB/s three time constants after the step",
            estimate.get(),
        );
    }

    /// Frames landing on the same instant must not divide by a zero interval.
    #[tokio::test(start_paused = true)]
    async fn a_burst_on_one_instant_is_survivable() {
        let mut estimate = RateEstimate::default();
        for _ in 0..1000 {
            estimate.observe(16 * 1024, TAU);
        }
        assert!(
            estimate.get().is_finite(),
            "{} is not finite",
            estimate.get()
        );
    }
}
