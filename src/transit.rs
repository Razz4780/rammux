//! Constants and small pieces of the transit control, see `TRANSIT_CONTROL.md`.

use std::time::Duration;

use tokio::time::Instant;

/// `k`: an allowance covers this many loops (smoothed round trip times) of the rate estimate.
pub const ALLOWANCE_RTTS: f64 = 2.0;

/// Bytes of credit at which the receiver returns credit to the sender, unless the base window is small.
///
/// Two frames of the default size: credit comes back at least every couple of frames, so the sender
/// does not wait for a half window, and the control frames stay a small share of the data (about 1700
/// per second at 150 Mbit/s with all of them counted, see "Known limits" in `TRANSIT_CONTROL.md`).
/// A tuning choice, not a derived value.
pub const CREDIT_THRESHOLD: u32 = 32 * 1024;

/// Weight of a new round-trip time sample in the smoothed one.
pub const SRTT_GAIN: f64 = 1.0 / 8.0;

/// The smallest smoothed round-trip time. Keeps the time constants of the allowances positive
/// with the microsecond resolution of the clock.
pub const SRTT_MIN: Duration = Duration::from_micros(1);

/// Returns `T_c`: how much credit has to accumulate on a stream before it is sent to the peer.
///
/// It is at most half of the base window, so a stream with less than `T_c` uncredited bytes
/// always has room in its base window, and credit cannot deadlock.
pub fn credit_threshold(base_window: u32) -> u64 {
    u64::from(CREDIT_THRESHOLD.min(base_window / 2).max(1))
}

/// Returns the part of `unacked` that comes from the shared pool.
pub fn borrowed(unacked: u64, base_window: u64) -> u64 {
    unacked.saturating_sub(base_window)
}

/// Moves the sum of borrowed bytes along with one stream's borrowed bytes.
pub fn rebalance(sum: &mut u64, old: u64, new: u64) {
    *sum = *sum - old + new;
}

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

    /// Makes the allowance (the estimate times `ALLOWANCE_RTTS * tau`) larger by `allowance` bytes.
    #[cfg(test)]
    pub fn grant_allowance(&mut self, allowance: f64, now: Instant, tau: f64) {
        self.add_amount(allowance / ALLOWANCE_RTTS, now, tau);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{
        conn_shared::ConnShared,
        transit::{RateEstimate, SRTT_MIN, borrowed, credit_threshold, rebalance},
    };
    use tokio::time::Instant;

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

    #[test]
    fn srtt_is_an_ewma_and_never_zero() {
        let mut global = ConnShared::default();
        assert_eq!(global.srtt, None);
        global.observe_rtt(Duration::from_millis(80));
        assert_eq!(global.srtt, Some(Duration::from_millis(80)));
        global.observe_rtt(Duration::from_millis(160));
        assert_eq!(global.srtt, Some(Duration::from_millis(90)));
        for _ in 0..200 {
            global.observe_rtt(Duration::ZERO);
        }
        assert_eq!(global.srtt, Some(SRTT_MIN));
    }

    #[test]
    fn threshold_is_half_the_window_at_most_32k() {
        assert_eq!(credit_threshold(64 * 1024), 32 * 1024);
        assert_eq!(credit_threshold(1024 * 1024), 32 * 1024);
        assert_eq!(credit_threshold(16 * 1024), 8 * 1024);
        assert_eq!(credit_threshold(12), 6);
        assert_eq!(credit_threshold(1), 1);
    }

    #[test]
    fn sums_do_not_overflow_u32() {
        // 32 768 streams that borrow 128 KiB each: 4 GiB.
        let mut sum = 0;
        for _ in 0..32_768 {
            rebalance(&mut sum, 0, borrowed(64 * 1024 + 128 * 1024, 64 * 1024));
        }
        assert_eq!(sum, 4 * 1024 * 1024 * 1024);
        assert!(sum > u64::from(u32::MAX));
    }

    #[test]
    fn borrowing_never_goes_below_zero() {
        assert_eq!(borrowed(10, 64), 0);
        assert_eq!(borrowed(64, 64), 0);
        assert_eq!(borrowed(100, 64), 36);
        let mut sum = 40;
        rebalance(&mut sum, 36, 0);
        assert_eq!(sum, 4);
        rebalance(&mut sum, 0, 10);
        assert_eq!(sum, 14);
    }
}
