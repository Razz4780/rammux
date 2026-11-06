//! The arithmetic of the flow control, see `TRANSIT_CONTROL.md`.
//!
//! Every stream has a fixed base window `B` in each direction. On top of it, the streams of one direction
//! share a [`Pool`]. A [`Window`] counts the bytes of one direction of one stream that are in flight
//! (sent, and not credited back yet), and keeps the usage of the pool in sync: the bytes beyond `B`
//! are borrowed from it.
//!
//! The sender decides how much of the pool a stream may borrow: in proportion to the rate at which the
//! reader of the stream proved to read ([`RateEstimate`]), over a few smoothed round trip times ([`Srtt`]).
//!
//! Nothing here knows about streams or frames.

use std::time::Duration;

pub use rate::RateEstimate;

mod rate;

/// `k`: an allowance covers this many loops (smoothed round trip times) of the rate estimate.
pub const ALLOWANCE_RTTS: f64 = 2.0;

/// Bytes of credit at which the receiver returns credit to the sender, unless the base window is small.
pub const CREDIT_THRESHOLD: u32 = 32 * 1024;

/// Weight of a new round trip time sample in the smoothed one.
pub const SRTT_GAIN: f64 = 1.0 / 8.0;

/// The smallest smoothed round trip time. Keeps the time constants of the allowances positive
/// with the microsecond resolution of the clock.
pub const SRTT_MIN: Duration = Duration::from_micros(1);

/// Returns `T_c`: how much credit has to accumulate on a stream before it is sent to the peer.
///
/// It is at most half of the base window, so a stream with less than `T_c` uncredited bytes
/// always has room in its base window, and credit cannot deadlock.
pub fn credit_threshold(base_window: u32) -> u64 {
    u64::from(CREDIT_THRESHOLD.min(base_window / 2).max(1))
}

/// Memory that the streams of one direction share on top of their base windows.
#[derive(Debug, Default)]
pub struct Pool {
    size: u64,
    /// Sum over the streams of the bytes they borrow, `U`.
    used: u64,
}

impl Pool {
    /// Creates an unused pool of the given size, `P`.
    pub fn new(size: u32) -> Self {
        Self {
            size: u64::from(size),
            used: 0,
        }
    }

    /// Returns the sum over the streams of the bytes they borrow.
    pub fn used(&self) -> u64 {
        self.used
    }

    /// Returns how many bytes are not borrowed.
    pub fn room(&self) -> u64 {
        self.size.saturating_sub(self.used)
    }

    /// Returns whether some bytes are not borrowed.
    pub fn has_room(&self) -> bool {
        self.room() > 0
    }

    /// Returns whether the streams borrow more than there is.
    ///
    /// The sender never lets that happen, so for a receiver it means that the peer violated the protocol.
    pub fn is_overdrawn(&self) -> bool {
        self.used > self.size
    }
}

/// Bytes of one direction of a stream that are in flight: sent and not credited back.
///
/// The part beyond the base window is borrowed from a [`Pool`].
#[derive(Debug)]
pub struct Window {
    /// The base window `B`.
    base: u64,
    unacked: u64,
}

impl Window {
    /// Creates a window with nothing in flight.
    pub fn new(base: u32) -> Self {
        Self {
            base: u64::from(base),
            unacked: 0,
        }
    }

    /// Returns the bytes in flight.
    pub fn unacked(&self) -> u64 {
        self.unacked
    }

    /// Returns the bytes in flight beyond the base window.
    pub fn borrowed(&self) -> u64 {
        Self::borrowed_at(self.unacked, self.base)
    }

    /// Returns how many more bytes fit into the base window.
    pub fn base_room(&self) -> u64 {
        self.base.saturating_sub(self.unacked)
    }

    /// Accounts for `len` more bytes in flight.
    pub fn add(&mut self, pool: &mut Pool, len: u64) {
        self.set(pool, self.unacked + len);
    }

    /// Sets the bytes in flight, and moves the usage of the pool along.
    ///
    /// Returns whether the stream gave some of the pool back.
    pub fn set(&mut self, pool: &mut Pool, unacked: u64) -> bool {
        let old = self.borrowed();
        let new = Self::borrowed_at(unacked, self.base);
        pool.used = pool.used - old + new;
        self.unacked = unacked;
        new < old
    }

    fn borrowed_at(unacked: u64, base: u64) -> u64 {
        unacked.saturating_sub(base)
    }
}

/// The smoothed round trip time of a connection.
#[derive(Debug, Default, Clone, Copy)]
pub struct Srtt(Option<Duration>);

impl Srtt {
    /// Returns the smoothed round trip time, [`None`] before the first sample. Never zero.
    pub fn get(self) -> Option<Duration> {
        self.0
    }

    /// Returns the smoothed round trip time in seconds: the time constant of the rate estimates, about one loop.
    pub fn secs(self) -> Option<f64> {
        self.0.map(|srtt| srtt.as_secs_f64())
    }

    /// Folds a sample into the smoothed value.
    ///
    /// The samples include our own queue and the peer's queue, and the delay of the peer's muxer.
    /// That is intended, see `TRANSIT_CONTROL.md`.
    pub fn observe(&mut self, rtt: Duration) {
        let srtt = match self.0 {
            None => rtt,
            Some(srtt) => {
                let srtt = srtt.as_secs_f64();
                Duration::from_secs_f64(srtt + (rtt.as_secs_f64() - srtt) * SRTT_GAIN)
            },
        };
        self.0 = Some(srtt.max(SRTT_MIN));
    }

    /// Creates a smoothed round trip time of the given value.
    #[cfg(test)]
    pub fn of(srtt: Duration) -> Self {
        Self(Some(srtt.max(SRTT_MIN)))
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::flow::{Pool, SRTT_MIN, Srtt, Window, credit_threshold};

    #[test]
    fn srtt_is_an_ewma_and_never_zero() {
        let mut srtt = Srtt::default();
        assert_eq!(srtt.get(), None);
        srtt.observe(Duration::from_millis(80));
        assert_eq!(srtt.get(), Some(Duration::from_millis(80)));
        srtt.observe(Duration::from_millis(160));
        assert_eq!(srtt.get(), Some(Duration::from_millis(90)));
        for _ in 0..200 {
            srtt.observe(Duration::ZERO);
        }
        assert_eq!(srtt.get(), Some(SRTT_MIN));
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
    fn pool_usage_does_not_overflow_u32() {
        // 32 768 streams that borrow 128 KiB each: 4 GiB.
        let mut pool = Pool::new(u32::MAX);
        for _ in 0..32_768 {
            Window::new(64 * 1024).add(&mut pool, 64 * 1024 + 128 * 1024);
        }
        assert_eq!(pool.used(), 4 * 1024 * 1024 * 1024);
        assert!(pool.is_overdrawn());
        assert_eq!(pool.room(), 0);
    }

    #[test]
    fn only_the_bytes_beyond_the_base_window_are_borrowed() {
        let mut pool = Pool::new(100);
        let mut window = Window::new(64);

        window.add(&mut pool, 10);
        assert_eq!(
            (window.borrowed(), window.base_room(), pool.used()),
            (0, 54, 0)
        );
        window.add(&mut pool, 54);
        assert_eq!(
            (window.borrowed(), window.base_room(), pool.used()),
            (0, 0, 0)
        );
        window.add(&mut pool, 36);
        assert_eq!((window.borrowed(), pool.used(), pool.room()), (36, 36, 64));

        let mut other = Window::new(64);
        other.add(&mut pool, 68);
        assert_eq!(pool.used(), 40);

        assert!(window.set(&mut pool, 0), "the pool share is given back");
        assert_eq!(pool.used(), 4);
        assert!(
            !window.set(&mut pool, 10),
            "nothing borrowed, nothing given back"
        );
        assert_eq!(pool.used(), 4);
    }
}
