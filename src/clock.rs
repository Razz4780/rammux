//! The private clock that timestamps `PING`s and `PONG`s.
//!
//! Each side counts microseconds since its own epoch, truncated to `u32`.
//! The value wraps every 2^32 microseconds (about 71.6 minutes),
//! so all arithmetic on timestamps and byte counters must wrap too.
//! Differences are read as `i32`, see [`diff`].

use std::time::Duration;

use tokio::time::Instant;

/// Converts [`Instant`]s into the wire timestamps of this side.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    epoch: Instant,
}

impl Clock {
    /// Creates a clock whose epoch is now.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    /// Returns the timestamp of the given instant.
    pub fn micros(&self, at: Instant) -> u32 {
        let micros = at.saturating_duration_since(self.epoch).as_micros();
        // Truncation to 32 bits is the point of the wire clock.
        u32::try_from(micros & u128::from(u32::MAX)).unwrap_or(0)
    }
}

/// Returns `a - b` of two wrapping `u32`s, read as `i32`.
///
/// `a` is later than `b` iff the result is positive.
/// This is the only correct way of comparing timestamps and counters.
pub fn diff(a: u32, b: u32) -> i32 {
    i32::from_ne_bytes(a.wrapping_sub(b).to_ne_bytes())
}

/// Returns the time between the timestamps `then` and `now` of one clock,
/// or [`None`] if `now` is earlier.
pub fn elapsed(now: u32, then: u32) -> Option<Duration> {
    u64::try_from(diff(now, then))
        .ok()
        .map(Duration::from_micros)
}

/// Hands out the `t_ping` values.
///
/// The values must strictly increase, so that an echoed `t_ping`
/// identifies one `PING` even if the clock has not moved.
#[derive(Default, Debug)]
pub struct PingStamps {
    last: Option<u32>,
}

impl PingStamps {
    /// Returns the stamp for a `PING` written at the given timestamp.
    ///
    /// The stamp only counts once it is [`committed`](Self::commit).
    pub fn next(&self, now: u32) -> u32 {
        match self.last {
            Some(last) if diff(now, last) <= 0 => last.wrapping_add(1),
            _ => now,
        }
    }

    /// Records that the given stamp went out.
    pub fn commit(&mut self, stamp: u32) {
        self.last = Some(stamp);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use rstest::rstest;

    use crate::clock::{Clock, PingStamps, diff, elapsed};

    #[rstest]
    #[case(5, 3, 2)]
    #[case(3, 5, -2)]
    #[case(1, u32::MAX, 2)]
    #[case(u32::MAX, 1, -2)]
    #[case(0x8000_0000, 0, i32::MIN)]
    #[test]
    fn diff_wraps(#[case] a: u32, #[case] b: u32, #[case] expected: i32) {
        assert_eq!(diff(a, b), expected);
    }

    #[test]
    fn elapsed_wraps_and_clamps() {
        assert_eq!(elapsed(10, 4), Some(Duration::from_micros(6)));
        assert_eq!(elapsed(4, 10), None);
        assert_eq!(elapsed(3, u32::MAX - 2), Some(Duration::from_micros(6)));
        assert_eq!(elapsed(7, 7), Some(Duration::ZERO));
    }

    #[tokio::test(start_paused = true)]
    async fn clock_truncates_to_u32() {
        let clock = Clock::new();
        assert_eq!(clock.micros(tokio::time::Instant::now()), 0);
        let wrap = Duration::from_micros(1 << 32);
        tokio::time::advance(wrap + Duration::from_micros(7)).await;
        assert_eq!(clock.micros(tokio::time::Instant::now()), 7);
    }

    #[test]
    fn stamps_strictly_increase() {
        let mut stamps = PingStamps::default();
        let first = stamps.next(100);
        assert_eq!(first, 100);
        // An uncommitted stamp does not count.
        assert_eq!(stamps.next(100), 100);
        stamps.commit(first);
        // The clock has not moved, or even went back.
        assert_eq!(stamps.next(100), 101);
        assert_eq!(stamps.next(90), 101);
        stamps.commit(101);
        assert_eq!(stamps.next(150), 150);
        // Across the wrap.
        stamps.commit(u32::MAX);
        assert_eq!(stamps.next(u32::MAX), 0);
        assert_eq!(stamps.next(2), 2);
    }
}
