//! The estimate of the rate at which the reader of a stream reads.

use tokio::time::Instant;

use crate::flow::ALLOWANCE_RTTS;

/// A decaying sum of credit, in bytes per second.
///
/// The senders use the smoothed round trip time as `tau`: the estimate averages over one loop.
///
/// With the time constant `tau`, the estimate of a steady flow of `r` bytes per second is `r`
/// and the sum of credit over the last `tau` seconds is `r * tau`: the sum is an exponentially
/// weighted window. Credit that is late, or bunched, is not lost: it only moves the estimate
/// for a while. Every credit counts, and the decay never stops.
#[derive(Debug, Clone, Copy)]
pub struct RateEstimate {
    rate: f64,
    at: Instant,
}

impl RateEstimate {
    /// Creates an estimate of zero.
    pub fn new(now: Instant) -> Self {
        Self { rate: 0.0, at: now }
    }

    /// Returns the estimate at `now`, in bytes per second.
    pub fn value(&self, now: Instant, tau: f64) -> f64 {
        let age = now.saturating_duration_since(self.at).as_secs_f64();
        self.rate * (-age / tau).exp()
    }

    /// Adds credit.
    pub fn add(&mut self, bytes: u32, now: Instant, tau: f64) {
        self.add_amount(f64::from(bytes), now, tau);
    }

    fn add_amount(&mut self, bytes: f64, now: Instant, tau: f64) {
        self.rate = self.value(now, tau) + bytes / tau;
        self.at = now;
    }

    /// Returns the allowance at `now`: how many bytes the stream may have borrowed,
    /// the estimate over `ALLOWANCE_RTTS` loops of `srtt` seconds.
    pub fn allowance(&self, now: Instant, srtt: f64) -> f64 {
        self.value(now, srtt) * ALLOWANCE_RTTS * srtt
    }

    /// Makes the allowance larger by `allowance` bytes.
    #[cfg(test)]
    pub fn grant_allowance(&mut self, allowance: f64, now: Instant, tau: f64) {
        self.add_amount(allowance / ALLOWANCE_RTTS, now, tau);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::flow::RateEstimate;

    #[tokio::test(start_paused = true)]
    async fn steady_credit_gives_the_rate() {
        // 1 MB/s in 1 KB steps every millisecond, tau = 100 ms.
        let tau = 0.1;
        let mut estimate = RateEstimate::new(Instant::now());
        for _ in 0..2000 {
            tokio::time::advance(Duration::from_millis(1)).await;
            estimate.add(1000, Instant::now(), tau);
        }
        let rate = estimate.value(Instant::now(), tau);
        assert!((rate - 1e6).abs() < 1e6 * 0.01, "{rate}");
        // The mean over tau: rate * tau bytes are in the window.
        assert!((rate * tau - 1e5).abs() < 1e3);
    }

    #[tokio::test(start_paused = true)]
    async fn batched_credit_ripples_by_about_half_a_batch() {
        // The same rate, 100 KB every 100 ms: the ripple is +-(batch / 2 tau) around the mean.
        let tau = 0.2;
        let mut estimate = RateEstimate::new(Instant::now());
        let (mut min, mut max) = (f64::MAX, 0.0_f64);
        for i in 0..200 {
            tokio::time::advance(Duration::from_millis(100)).await;
            estimate.add(100_000, Instant::now(), tau);
            if i > 100 {
                max = max.max(estimate.value(Instant::now(), tau));
                // Just before the next batch.
                let before = Instant::now() + Duration::from_millis(100);
                min = min.min(estimate.value(before, tau));
            }
        }
        let mean = 1e6;
        let ripple = 100_000.0 / (2.0 * tau);
        assert!(max < mean + 1.2 * ripple + 1e4 && max > mean, "{max}");
        assert!(min > mean - 1.2 * ripple - 1e4 && min < mean, "{min}");
    }

    #[tokio::test(start_paused = true)]
    async fn estimate_decays_after_a_pause() {
        let tau = 0.05;
        let mut estimate = RateEstimate::new(Instant::now());
        estimate.add(100_000, Instant::now(), tau);
        let start = estimate.value(Instant::now(), tau);
        assert!((start - 2e6).abs() < 1.0);
        tokio::time::advance(Duration::from_secs_f64(tau)).await;
        let after_tau = estimate.value(Instant::now(), tau);
        assert!((after_tau - start / std::f64::consts::E).abs() < 1.0);
        tokio::time::advance(Duration::from_secs_f64(tau * 20.0)).await;
        assert!(estimate.value(Instant::now(), tau) < start * 1e-8);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_followed_by_a_burst_is_not_lost() {
        // The flow of 1 MB/s stalls for tau, then everything arrives at once.
        let tau = 0.1;
        let mut steady = RateEstimate::new(Instant::now());
        let mut bursty = RateEstimate::new(Instant::now());
        for _ in 0..1000 {
            tokio::time::advance(Duration::from_millis(1)).await;
            steady.add(1000, Instant::now(), tau);
            bursty.add(1000, Instant::now(), tau);
        }
        // Stall: the steady flow carries on, the bursty one gets the credit late.
        let mut late = 0;
        for _ in 0..100 {
            tokio::time::advance(Duration::from_millis(1)).await;
            steady.add(1000, Instant::now(), tau);
            late += 1000;
        }
        bursty.add(late, Instant::now(), tau);
        let (steady, bursty) = (
            steady.value(Instant::now(), tau),
            bursty.value(Instant::now(), tau),
        );
        // The overshoot is below the 37% bound, the sums agree up to the decay of the stalled credit.
        assert!(
            bursty > steady && bursty < steady * 1.37,
            "{bursty} {steady}"
        );
    }
}
