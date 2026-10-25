//! Constants and small pieces of the transit control, see `TRANSIT_CONTROL.md`.

/// Bytes of credit at which the receiver returns credit to the sender, unless the base window is small.
pub const CREDIT_THRESHOLD: u32 = 32 * 1024;

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
    use crate::transit::{borrowed, credit_threshold, rebalance};

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
