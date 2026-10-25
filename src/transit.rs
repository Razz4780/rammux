//! Constants and small pieces of the transit control, see `TRANSIT_CONTROL.md`.

use std::time::Duration;

/// Bytes of credit at which the receiver returns credit to the sender, unless the base window is small.
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{
        global_pool::GlobalPool,
        transit::{SRTT_MIN, borrowed, credit_threshold, rebalance},
    };

    #[test]
    fn srtt_is_an_ewma_and_never_zero() {
        let mut global = GlobalPool::default();
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
