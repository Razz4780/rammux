//! The one-way delay signal, and why the window is steered on it.
//!
//! A round trip is the obvious thing to measure and the wrong one. It sums the
//! queues in *both* directions, so on a connection that is busy each way - an
//! echo, say - a sender reads a round trip inflated by the queue its peer is
//! standing in, which tells it nothing about the one its own window governs.
//! Gating growth on round-trip delay was tried in an earlier design and froze
//! whichever side was busiest, which is exactly the side that needed to grow.
//!
//! A one-way delay is attributable to a single direction, and the direction it
//! describes is the one the receiver's window is responsible for. It costs a
//! timestamp per `DATA` frame and no clock synchronisation at all: the offset
//! between two unsynchronised clocks lands in every sample identically, so
//! subtracting a minimum over recent samples removes it and leaves the queue.

use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;

/// A minimum of one-way delay over a sliding window, in microseconds.
///
/// Sliding rather than all-time, because the two things that would otherwise
/// pin the base below anything reachable are both real: a path whose
/// propagation delay genuinely changed, and a clock that drifted.
///
/// # Why the memory is what it is
///
/// libutp keeps thirteen one-minute buckets and corrects for clock skew by
/// watching the *other* direction's base creep. Neither is needed here, and for
/// the same reason: the link-clearing probe. It empties the link every couple
/// of seconds, and the first frames after it carry the true propagation delay,
/// so the base is re-established far more often than a clock can drift by
/// anything the window would notice (libutp's own figure is 10 ms per 325 s).
///
/// That makes the memory a *dependency on the probe*, and it has to be longer
/// than the probe interval by a comfortable margin. A memory shorter than the
/// gap between clean samples fails in a specific way: a queue that never
/// empties becomes the base once the clean samples age out, reads as zero
/// queue, and the window ratchets up by one target every memory-length.
/// A minute against the default 8 s probe leaves about seven clean samples in
/// memory at any time, so a missed exchange or two costs nothing.
#[derive(Debug)]
struct Base {
    buckets: [i64; Self::BUCKETS],
    /// Where the newest bucket is.
    head: usize,
    opened: Instant,
}

impl Default for Base {
    fn default() -> Self {
        Self {
            buckets: [i64::MAX; Self::BUCKETS],
            head: 0,
            opened: Instant::now(),
        }
    }
}

impl Base {
    const BUCKETS: usize = 60;
    const BUCKET: Duration = Duration::from_secs(1);

    fn observe(&mut self, owd: i64) {
        let elapsed = self.opened.elapsed();
        if elapsed >= Self::BUCKET {
            // Every bucket between then and now is empty. Past a full lap the
            // whole memory is, and there is no point walking further.
            let steps = (elapsed.as_secs() as usize).min(Self::BUCKETS);
            for _ in 0..steps {
                self.head = (self.head + 1) % Self::BUCKETS;
                self.buckets[self.head] = i64::MAX;
            }
            self.opened += Self::BUCKET * elapsed.as_secs() as u32;
        }
        self.buckets[self.head] = self.buckets[self.head].min(owd);
    }

    fn get(&self) -> Option<i64> {
        self.buckets
            .iter()
            .copied()
            .min()
            .filter(|min| *min != i64::MAX)
    }
}

/// Queuing delay on one direction, filtered enough to steer a window with.
///
/// Two filters, and both are load-bearing.
///
/// *Within* a control interval the statistic is the **minimum**, as RFC 6817
/// specifies, because the minimum is the standing queue and nothing else. A
/// byte-weighted mean was tried instead, on the theory that it matches the
/// delay an arbitrary byte waits: it does not work, because a window-limited
/// sender releases a re-grant's worth at a time and the mean is then dominated
/// by each burst serialising behind itself. That term grows with the window
/// whether or not any queue is standing, so the loop settles wherever
/// burst-serialisation equals the target - measured, 34 KiB against a 50 KiB
/// bandwidth-delay product, at 3.4 of 9.3 Mbit.
///
/// *Across* intervals the statistic is a **median of the last few**, because a
/// single interval's minimum is far too noisy to act on: measured on a 10 Mbit
/// path it alternates between about 2 ms and about 35 ms from one round trip
/// to the next, depending on whether the sender happened to leave a gap. Acting
/// on the newest reading alone left one run in four settling at three times the
/// latency of the others; a median over four intervals removed the spread
/// entirely. A median rather than a minimum-of-minima, which would bias the
/// estimate down and grow the window on the strength of the one quietest
/// moment in four round trips.
#[derive(Debug)]
pub(crate) struct Delay {
    base: Base,
    /// Smallest delay seen since the interval opened.
    interval: Option<i64>,
    /// Recent intervals' minima, newest first.
    history: VecDeque<i64>,
    /// How many of those the median is taken over.
    filter: usize,
}

impl Delay {
    pub(crate) fn new(filter: usize) -> Self {
        Self {
            base: Base::default(),
            interval: None,
            history: VecDeque::new(),
            filter: filter.max(1),
        }
    }

    /// Folds in one frame's one-way delay, offset and all.
    pub(crate) fn observe(&mut self, owd: i64) {
        self.base.observe(owd);
        self.interval = Some(self.interval.map_or(owd, |min| min.min(owd)));
    }

    /// Closes the interval and returns the queuing delay to steer on.
    ///
    /// `None` until a base and at least one sample exist, which is also what
    /// keeps the control law from acting on an empty interval.
    pub(crate) fn sample(&mut self) -> Option<Duration> {
        let (base, interval) = (self.base.get()?, self.interval.take()?);
        self.history.push_front((interval - base).max(0));
        self.history.truncate(self.filter);
        let mut recent: Vec<i64> = self.history.iter().copied().collect();
        recent.sort_unstable();
        Some(Duration::from_micros(recent[recent.len() / 2] as u64))
    }

    /// Throws away the open interval.
    ///
    /// For when the connection disturbed the link itself - a link-clearing
    /// probe empties it on purpose, so the frames either side of one carry the
    /// probe's own drain rather than whatever queue was standing.
    pub(crate) fn discard_interval(&mut self) {
        self.interval = None;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Puts a base in memory and closes the interval it arrived in, which is
    /// what the control loop does once per round trip. Without the close, the
    /// base sample is still the smallest thing in the open interval and every
    /// queue reads as zero.
    async fn base_of(delay: &mut Delay, owd: i64) {
        delay.observe(owd);
        delay.discard_interval();
        tokio::time::advance(Duration::from_millis(1500)).await;
    }

    /// The whole reason no clock synchronisation is needed: the offset between
    /// two unsynchronised clocks is in every sample, so it cancels.
    #[tokio::test(start_paused = true)]
    async fn the_base_cancels_an_arbitrary_clock_offset() {
        let mut delay = Delay::new(1);
        assert_eq!(delay.sample(), None, "reported a delay before any sample");

        base_of(&mut delay, 1_000_000).await;
        for queue in [3, 11, 7] {
            delay.observe(1_000_000 + queue);
        }
        assert_eq!(
            delay.sample(),
            Some(Duration::from_micros(3)),
            "the offset leaked into the queuing delay"
        );
    }

    /// Within an interval the statistic is the minimum, so one late frame is
    /// not supposed to read as a queue.
    #[tokio::test(start_paused = true)]
    async fn an_interval_reports_its_minimum() {
        let mut delay = Delay::new(1);
        base_of(&mut delay, 100).await;
        for owd in [140, 900, 210] {
            delay.observe(owd);
        }
        assert_eq!(delay.sample(), Some(Duration::from_micros(40)));
    }

    /// A base older than the memory has to go, or a path whose propagation
    /// delay went up would read as a permanent queue. This also covers the
    /// rollover after a long idle, which walks at most one lap of buckets
    /// however long the idle was.
    #[tokio::test(start_paused = true)]
    async fn the_base_forgets_what_is_older_than_its_memory() {
        let mut delay = Delay::new(1);
        base_of(&mut delay, 0).await;
        tokio::time::advance(Duration::from_secs(600)).await;

        delay.observe(1_000);
        delay.discard_interval();
        delay.observe(1_000);
        assert_eq!(
            delay.sample(),
            Some(Duration::ZERO),
            "a base from ten minutes ago still counted"
        );
    }

    /// Across intervals the statistic is a median, because acting on the
    /// newest reading alone let one noisy interval in four move the window.
    #[tokio::test(start_paused = true)]
    async fn the_median_rejects_a_single_outlying_interval() {
        let mut delay = Delay::new(4);
        base_of(&mut delay, 0).await;

        let mut last = None;
        for interval in [10, 12, 5_000, 11] {
            delay.observe(interval);
            last = delay.sample();
        }
        assert!(
            last.is_some_and(|queued| queued < Duration::from_micros(100)),
            "a single 5 ms interval reached the control law as {last:?}"
        );
    }
}
