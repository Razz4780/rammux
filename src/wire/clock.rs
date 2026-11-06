//! The private clock that stamps `PING`s.
//!
//! Each side counts microseconds since its own epoch. On the wire, `t_ping` and `t_echo` carry only
//! the low 32 bits, which wrap every 2^32 microseconds (about 71.6 minutes). This side keeps its
//! timestamps at their full 64 bits, and only the wire values wrap. An echo is extended back to the full
//! stamp of the `PING` it answers, see [`PingStamps::sent`]. Nothing compares wrapped values, so gaps of
//! any length (an idle connection, a peer that does not answer for an hour) are harmless.

use tokio::time::Instant;

/// How far back an echo may reach from the newest `PING`, in microseconds: half of the range of the
/// wire value, about 35.8 minutes.
///
/// An echo that reaches further back is indistinguishable from an echo of a `PING` that was never sent.
const ECHO_REACH: u64 = 1 << 31;

/// Converts [`Instant`]s into the timestamps of this side.
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

    /// Returns the timestamp of the given instant, in microseconds since the epoch.
    pub fn micros(&self, at: Instant) -> u64 {
        let micros = at.saturating_duration_since(self.epoch).as_micros();
        u64::try_from(micros).unwrap_or(u64::MAX)
    }
}

/// Returns the wire value of a timestamp: its low 32 bits.
pub fn to_wire(micros: u64) -> u32 {
    // Truncation is the point of the wire value.
    #[allow(clippy::cast_possible_truncation)]
    let wire = micros as u32;
    wire
}

/// Hands out the stamps of the `PING`s, and recognizes their echoes.
///
/// The stamps strictly increase, so that an echo identifies one `PING` even if the clock has not moved.
#[derive(Default, Debug)]
pub struct PingStamps {
    /// The stamp of the newest `PING` that went out.
    newest: Option<u64>,
}

impl PingStamps {
    /// Returns the stamp for a `PING` written at the given time.
    ///
    /// The stamp only counts once it is [committed](Self::commit).
    pub fn next(&self, now: u64) -> u64 {
        match self.newest {
            Some(newest) if now <= newest => newest + 1,
            _ => now,
        }
    }

    /// Records that the `PING` with the given stamp went out.
    pub fn commit(&mut self, stamp: u64) {
        self.newest = Some(stamp);
    }

    /// Returns the full stamp of the `PING` that an echo answers.
    ///
    /// Returns [`None`] if the echo cannot be one of ours: no `PING` went out yet, the echo is later than
    /// the newest `PING`, or it reaches further back than [`ECHO_REACH`].
    pub fn sent(&self, t_echo: u32) -> Option<u64> {
        let newest = self.newest?;
        let back = u64::from(to_wire(newest).wrapping_sub(t_echo));
        if back >= ECHO_REACH {
            return None;
        }
        newest.checked_sub(back)
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::wire::clock::{Clock, ECHO_REACH, PingStamps, to_wire};

    /// 2^32 microseconds, where the wire value wraps.
    const WRAP: u64 = 1 << 32;

    #[tokio::test(start_paused = true)]
    async fn the_clock_does_not_wrap() {
        let clock = Clock::new();
        assert_eq!(clock.micros(tokio::time::Instant::now()), 0);
        tokio::time::advance(Duration::from_micros(WRAP + 7)).await;
        let micros = clock.micros(tokio::time::Instant::now());
        assert_eq!(micros, WRAP + 7);
        assert_eq!(to_wire(micros), 7);
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
    }

    #[test]
    fn stamps_follow_the_clock_after_any_pause() {
        let mut stamps = PingStamps::default();
        stamps.commit(100);
        for pause in [
            ECHO_REACH - 1,
            ECHO_REACH,
            ECHO_REACH + 1,
            WRAP - 1,
            WRAP,
            3 * WRAP,
        ] {
            assert_eq!(stamps.next(100 + pause), 100 + pause);
        }
    }

    #[test]
    fn an_echo_is_extended_to_the_full_stamp() {
        let mut stamps = PingStamps::default();
        assert_eq!(stamps.sent(5), None, "no PING went out");

        let newest = 3 * WRAP + 1000;
        stamps.commit(newest);
        assert_eq!(stamps.sent(1000), Some(newest));
        assert_eq!(stamps.sent(400), Some(newest - 600));
        // Across the wrap of the wire value.
        let before_wrap = to_wire(newest - 2000);
        assert_eq!(stamps.sent(before_wrap), Some(newest - 2000));
        // As far back as the reach.
        let oldest = to_wire(newest - (ECHO_REACH - 1));
        assert_eq!(stamps.sent(oldest), Some(newest - (ECHO_REACH - 1)));
    }

    #[test]
    fn an_echo_that_cannot_be_ours_is_recognized() {
        let mut stamps = PingStamps::default();
        let newest = 3 * WRAP + 1000;
        stamps.commit(newest);
        // Later than the newest PING.
        assert_eq!(stamps.sent(1001), None);
        assert_eq!(stamps.sent(to_wire(newest + ECHO_REACH)), None);
        // Further back than the reach.
        assert_eq!(stamps.sent(to_wire(newest - ECHO_REACH)), None);

        // Before the epoch.
        stamps.commit(1000);
        assert_eq!(stamps.sent(to_wire(WRAP - 5)), None);
    }
}
