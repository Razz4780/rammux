//! The private clock that timestamps `PING`s and `PONG`s.
//!
//! Each side counts microseconds since its own epoch, truncated to `u32`.
//! The value wraps every 2^32 microseconds (about 71.6 minutes),
//! so all arithmetic on timestamps must wrap too.

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

/// Returns the time from the timestamp `then` to the later timestamp `now` of one clock.
///
/// The difference wraps, so it is right as long as `now` is less than 2^32 microseconds after `then`.
pub fn elapsed(now: u32, then: u32) -> Duration {
    Duration::from_micros(u64::from(now.wrapping_sub(then)))
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::wire::clock::{Clock, elapsed};

    #[test]
    fn elapsed_wraps() {
        assert_eq!(elapsed(10, 4), Duration::from_micros(6));
        assert_eq!(elapsed(3, u32::MAX - 2), Duration::from_micros(6));
        assert_eq!(elapsed(7, 7), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn clock_truncates_to_u32() {
        let clock = Clock::new();
        assert_eq!(clock.micros(Instant::now()), 0);
        let wrap = Duration::from_micros(1 << 32);
        tokio::time::advance(wrap + Duration::from_micros(7)).await;
        assert_eq!(clock.micros(Instant::now()), 7);
    }
}
