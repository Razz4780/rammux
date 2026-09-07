//! Sizing the window a receiver grants, and returning credit for it.
//!
//! # The loop everything is judged by
//!
//! A re-grant of `T` bytes cannot reach the sender until `T / rate` has passed,
//! so the credit loop is `R = RTT + T / rate + queueing`. Two things follow,
//! and between them they explain every constant in this file:
//!
//! * While the window binds, throughput is `W / R`. A coarse `T` is therefore
//!   paid for in window, and window above the bandwidth-delay product is paid
//!   for in latency, at `(W - BDP) / rate`.
//! * Any rule that sizes `W` from the rate it observes is sizing it from
//!   `W / R`, which is an underestimate *exactly* while the window is the
//!   thing that needs to grow.
//!
//! # Why the re-grant threshold is paced
//!
//! Re-granting at half the window makes `T` proportional to `W`, so no window
//! below `2 x BDP` can be served. Re-granting at a flat 64 KiB fixes that at
//! the top of the range and not at the bottom: 64 KiB is half a millisecond at
//! 1 Gbit and 56 milliseconds at 10 Mbit, where it is longer than the round
//! trip it is added to. Measured on a 10 Mbit, 40 ms path at a fixed 128 KiB
//! window, dropping `T` from 64 KiB to 16 KiB moved the link from 7.0 to
//! 9.3 Mbit and the echo from 153 to 108 ms.
//!
//! So `T` is paced at [`Sizing::re_grants_per_rtt`] per round trip, which holds
//! the delay to the same share of `RTT` on every link. It costs one 8 byte
//! header per `T` bytes: 0.2% of the link at [`RE_GRANT_FLOOR`], less above it.
//!
//! # Why the window may only come down one way
//!
//! Shrinking by *revoking* credit was tried in an earlier design, with a
//! "debt" the sender had to honour, and stopped connections at a fifth of the
//! pipe - unsurprisingly, since the sender had already spent what it was being
//! asked to give back. Here a window comes down only by handing back less than
//! was freed, so a single re-grant can shrink it by at most what it just freed
//! and deeper cuts arrive over the following few. Nothing is ever taken away.

use std::time::Duration;

use tokio::time::Instant;

use crate::{
    delay::Delay,
    rate::{self, RateEstimate},
};

/// Smallest re-grant the pacing rule will ask for.
///
/// A floor on frame count rather than on bandwidth: one 8 byte header per
/// re-grant is 0.2% of the link even here, and finer than this buys nothing
/// measurable while costing a packet per re-grant on a fast path.
pub const RE_GRANT_FLOOR: u32 = 4 * 1024;

/// Smallest window [`Growth::Ledbat`] may shrink to.
///
/// Below about this the re-grant cadence is all that is left of the window.
const MIN_WINDOW: u32 = 16 * 1024;

/// Control interval for [`Growth::Ledbat`] before a round trip is known.
///
/// The rule is otherwise clocked at one round trip, because that is how long a
/// change to the window takes to show up in the delay it is steering. Acting
/// faster than the loop's own lag is what makes a delay controller oscillate.
const DEFAULT_CONTROL_INTERVAL: Duration = Duration::from_millis(50);

/// How the granted window is sized.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Growth {
    /// Delay-targeting, after LEDBAT (RFC 6817): hold a fixed amount of
    /// one-way queuing delay on the direction this window governs.
    ///
    /// It estimates no bandwidth, so the circularity above does not apply. What
    /// it reads instead is the queue its own window is producing, which is the
    /// quantity the whole design is trying to keep small.
    ///
    /// "After" LEDBAT, not LEDBAT. Two departures are deliberate, and both
    /// follow from the goal being the opposite of LEDBAT's: this is the only
    /// flow on its link and wants all of it, where LEDBAT is a background
    /// protocol that yields. So the gain is *multiplicative* - `gain` of the
    /// window per round trip - where RFC 6817 and libutp add at most one or
    /// two packets per round trip and would take thousands of them to fill a
    /// fast path. And the target is a fraction of the round trip rather than
    /// libutp's flat 100 ms, because 100 ms is the whole latency budget on
    /// most of the links this was tuned on. What is kept from LEDBAT is what
    /// makes it work: the one-way signal, the minimum-filtered sample, the
    /// base that cancels the clock offset, and the rule that growth needs the
    /// window to have actually been the limit (libutp's
    /// `last_maxed_out_window`; here the peer's starved flag).
    ///
    /// It needs two signals, not one. The delay says when the window is too
    /// big; the peer reporting that it ran out of credit says when it is too
    /// small. Delay alone cannot tell, because a byte waiting for credit has
    /// not entered the path yet and so contributes no delay at all - a window
    /// shrinking towards nothing looks exactly like a link with no queue on
    /// it. Measured, delay alone settled at 34 KiB against a 50 KiB
    /// bandwidth-delay product and 3.4 of 9.3 Mbit, with 300 ms of echo
    /// latency the signal could not see.
    ///
    /// Alone among the rules it can come *down*, which is what lets it serve a
    /// link whose product is below the initial window: on a 10 Mbit, 40 ms
    /// path the 128 KiB default start is already 2.6 times the product, and a
    /// grow-only rule is stuck there for the life of the connection.
    Ledbat {
        /// Queuing delay to hold, as a fraction of the clean round trip, or
        /// zero to use `target` instead.
        ///
        /// Measured, the point where more queue stops buying throughput sits
        /// at 0.3 to 0.4 of the round trip on every link tried - 8 ms on a
        /// 20 ms path, 12 ms on a 40 ms one, past 20 ms on a 60 ms one. A flat
        /// target cannot sit at that knee on more than one of them, which is
        /// why a flat 5 ms left the two long-round-trip links giving up the
        /// most throughput.
        target_rtts: f64,
        /// Queuing delay to hold when `target_rtts` is zero. Zero would be the
        /// bandwidth-delay product exactly, with no margin for the sender to
        /// be late.
        target: Duration,
        /// Fraction of the window a full-scale error moves it by, per control
        /// interval.
        ///
        /// The loop has a round trip of lag in it - that is how long a change
        /// takes to show up in the delay it is steering - so the gain has to
        /// be small. Measured, 0.5 oscillated the window between a third and
        /// twice its settling point on every link.
        gain: f64,
    },
}

impl Default for Growth {
    /// The delay rule, at the settings it was tuned to.
    ///
    /// It won on every link measured - lowest latency on three of four and
    /// within a few percent of the best throughput - and it is the only rule
    /// that can bring a window *down*, so it is the only one that serves a
    /// path whose bandwidth-delay product is below the initial window.
    fn default() -> Self {
        Self::Ledbat {
            // The knee where more queue stops buying throughput sits at about
            // 0.3 of the round trip on every link tried.
            target_rtts: 0.30,
            // Only used when `target_rtts` is zero.
            target: Duration::from_millis(5),
            // 0.5 oscillated on every link; 0.1 is where it settled.
            gain: 0.1,
        }
    }
}

/// The bounds and cadences a window is sized within.
///
/// [`Sizing::default`] is the tuned configuration, and the command line's
/// defaults are read from it rather than repeated, so the two cannot drift.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sizing {
    /// Granted before anything has been measured.
    pub initial: u32,
    /// Growth limit.
    pub max: u32,
    /// Upper bound on the freed credit that triggers a re-grant. Capped again
    /// at half the window, so a small window keeps the classic half-window
    /// cadence rather than re-granting more than it has outstanding.
    pub re_grant: u32,
    /// Re-grants to fit into a round trip, or zero to leave the cadence at the
    /// flat `re_grant` on every link.
    pub re_grants_per_rtt: u32,
    /// Control intervals the delay median is taken over.
    pub delay_filter: usize,
    /// The rule that sizes the granted window.
    pub growth: Growth,
}

impl Default for Sizing {
    fn default() -> Self {
        Self {
            // Large enough that a fast path is not starved through its ramp;
            // the delay rule brings it down where it is too much.
            initial: 128 * 1024,
            // Above the largest bandwidth-delay product measured, so it never
            // binds on the links tried. The socket buffer ceilings the harness
            // sets are sized to stay clear of this.
            max: 16 * 1024 * 1024,
            // The flat cap on a re-grant; pacing usually asks for less.
            re_grant: 64 * 1024,
            // 32 per round trip holds the re-grant's delay to a thirty-second
            // of the round trip; 8 was measurably coarser on the long paths.
            re_grants_per_rtt: 32,
            // Four intervals removed the run-to-run spread entirely; one left
            // one run in four settling at three times the latency.
            delay_filter: 4,
            growth: Default::default(),
        }
    }
}

/// The window one end grants the other, and everything that sizes it.
#[derive(Debug)]
pub(crate) struct Window {
    sizing: Sizing,
    /// What the peer is currently allowed to have in flight.
    granted: u32,
    /// Freed since the last re-grant.
    freed: u32,
    /// Everything ever granted and everything ever received, so that a peer
    /// sending past its credit can be told apart from one that is merely fast.
    granted_total: u64,
    received_total: u64,
    /// What [`Growth::Ledbat`] is steering towards; the re-grant walks
    /// `granted` towards it as freed credit allows.
    desired: u32,
    rate: RateEstimate,
    delay: Delay,
    /// Last queuing delay the control law acted on, for the trace log.
    queued: Option<Duration>,
    /// Whether the peer has reported running out of credit this control
    /// interval - the only evidence that a bigger window would be used.
    peer_starved: bool,
    controlled_at: Instant,
}

impl Window {
    pub(crate) fn new(sizing: Sizing) -> Self {
        let now = Instant::now();
        Self {
            sizing,
            granted: sizing.initial,
            freed: 0,
            granted_total: u64::from(sizing.initial),
            received_total: 0,
            desired: sizing.initial,
            rate: RateEstimate::default(),
            delay: Delay::new(sizing.delay_filter),
            queued: None,
            peer_starved: false,
            controlled_at: now,
        }
    }

    pub(crate) fn granted(&self) -> u32 {
        self.granted
    }

    pub(crate) fn desired(&self) -> u32 {
        self.desired
    }

    pub(crate) fn rate(&self) -> f64 {
        self.rate.get()
    }

    pub(crate) fn queued(&self) -> Option<Duration> {
        self.queued
    }

    /// Frees credit for payload that has arrived, and feeds the rate estimate.
    ///
    /// Credit is freed on *arrival*, not on the application reading it,
    /// because arrival is what ends a byte's time in flight.
    pub(crate) fn on_payload(&mut self, len: u32, rtt: Option<Duration>) {
        self.received_total += u64::from(len);
        self.freed = self.freed.saturating_add(len);
        self.rate.observe(u64::from(len), rate::tau(rtt));
    }

    /// Whether the peer has sent more than it was ever granted.
    pub(crate) fn overdrawn(&self) -> bool {
        self.received_total > self.granted_total
    }

    /// Folds in one frame's one-way delay.
    pub(crate) fn on_delay(&mut self, owd: i64) {
        self.delay.observe(owd);
    }

    /// Records that the peer wrote a frame after running out of credit.
    pub(crate) fn note_peer_starved(&mut self) {
        self.peer_starved = true;
    }

    /// Restarts the delay clock a link-clearing probe disturbed.
    ///
    /// A probe empties the link on purpose, so the frames either side of one
    /// carry the probe's own drain rather than whatever queue was standing;
    /// the interval they landed in is thrown away and the control clock
    /// restarts. Left running, the pause this connection inflicted on itself
    /// would read as a change in the queue.
    pub(crate) fn on_probe_end(&mut self) {
        self.controlled_at = Instant::now();
        self.delay.discard_interval();
    }

    /// Runs the delay control law, if its interval has come round.
    ///
    /// Deliberately not folded into [`Self::take_grant`]. That is skipped
    /// while undelivered payload stands at a window's worth, which is exactly
    /// when an application busy enough to stop reading is also the one whose
    /// queue most needs steering. Measured, the coupling stretched the control
    /// interval from one round trip to about eight, and a window reading 57 ms
    /// of queue against a 5 ms target took over a second to come down 15%.
    pub(crate) fn steer(&mut self, rtt: Option<Duration>) {
        let Growth::Ledbat {
            target_rtts,
            target,
            gain,
        } = self.sizing.growth;
        let interval = rtt.unwrap_or(DEFAULT_CONTROL_INTERVAL);
        if self.controlled_at.elapsed() < interval {
            return;
        }
        let Some(queued) = self.delay.sample() else {
            return;
        };
        self.controlled_at = Instant::now();
        self.queued = Some(queued);
        let wanted_starved = std::mem::take(&mut self.peer_starved);

        let target = if target_rtts > 0.0 {
            interval.as_secs_f64() * target_rtts
        } else {
            target.as_secs_f64()
        };
        // The error as a fraction of the target, so a link with no queue on it
        // grows at the full gain and one at twice the target shrinks as fast.
        let off_target = ((target - queued.as_secs_f64()) / target).clamp(-1.0, 1.0);
        // Shrinking on an over-target queue always applies; growing needs the
        // peer to have said it wanted the credit, or an idle direction - one
        // that can only return what the other delivers - reads no queue and
        // grows into the cap.
        let off_target = if wanted_starved {
            off_target
        } else {
            off_target.min(0.0)
        };
        self.desired = scale(self.desired, 1.0 + gain * off_target)
            .clamp(MIN_WINDOW.min(self.sizing.max), self.sizing.max);
    }

    /// The credit to grant now, if a re-grant is due.
    ///
    /// `undelivered` is payload parsed but not yet read by the application.
    /// Holding the grant back while that stands at a window's worth is what
    /// bounds memory - never declining to read the socket, which would also
    /// decline the credit this end is waiting for and deadlock rather than
    /// apply backpressure.
    pub(crate) fn take_grant(&mut self, undelivered: usize, rtt: Option<Duration>) -> Option<u32> {
        if self.freed < self.re_grant_threshold(rtt) || undelivered >= self.granted as usize {
            return None;
        }
        let target = self.resize();
        // A window may only come down by as much as this re-grant freed; see
        // the module header for why it may not come down any other way.
        let floor = self.granted - self.freed.min(self.granted.saturating_sub(target));
        let granted = target.max(floor);
        // One frame carries both halves: the credit that was freed, and
        // whatever the window's change adds to or subtracts from it. `floor`
        // guarantees the subtraction cannot go below zero.
        let update = (self.freed.saturating_add(granted)) - self.granted;
        self.granted = granted;
        self.granted_total += u64::from(update);
        self.freed = 0;
        (update > 0).then_some(update)
    }

    /// How much freed credit triggers a re-grant.
    fn re_grant_threshold(&self, rtt: Option<Duration>) -> u32 {
        // Never more than half the window: a step bigger than that cannot get
        // back before the sender has drained what it was holding.
        let flat = self.sizing.re_grant.min(self.granted / 2).max(1);
        let (Some(rtt), true) = (rtt, self.sizing.re_grants_per_rtt > 0) else {
            return flat;
        };
        if self.rate.get() <= 0.0 {
            return flat;
        }
        let paced = self.rate.get() * rtt.as_secs_f64() / f64::from(self.sizing.re_grants_per_rtt);
        let paced = if paced >= f64::from(u32::MAX) {
            u32::MAX
        } else {
            paced as u32
        };
        paced.clamp(RE_GRANT_FLOOR.min(flat), flat)
    }

    /// The window size this rule wants now. May be below the current size.
    fn resize(&mut self) -> u32 {
        let target = self.desired;
        let floor = MIN_WINDOW;
        target.clamp(floor.min(self.sizing.max), self.sizing.max)
    }
}

/// `window * factor`, saturating rather than wrapping into nonsense.
fn scale(window: u32, factor: f64) -> u32 {
    scale_f64(f64::from(window) * factor).max(1)
}

fn scale_f64(value: f64) -> u32 {
    if value >= f64::from(u32::MAX) {
        u32::MAX
    } else if value <= 0.0 {
        0
    } else {
        value as u32
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const RTT: Duration = Duration::from_millis(40);

    fn sizing(growth: Growth, initial: u32) -> Sizing {
        Sizing {
            initial,
            max: 8 * 1024 * 1024,
            re_grant: 64 * 1024,
            re_grants_per_rtt: 0,
            delay_filter: 1,
            growth,
        }
    }

    fn ledbat(gain: f64) -> Growth {
        Growth::Ledbat {
            target_rtts: 0.0,
            target: Duration::from_millis(5),
            gain,
        }
    }

    /// The base has to come from an interval that has been closed, or it is
    /// still the smallest thing in the open one and every queue reads as zero.
    /// A probe closes it, which is also how it happens on a real connection.
    async fn with_base(window: &mut Window) {
        window.on_delay(1_000_000);
        window.on_probe_end();
        tokio::time::advance(Duration::from_millis(1500)).await;
    }

    /// Feeds the rate estimate a known bytes-per-second.
    async fn at_rate(window: &mut Window, bytes_per_second: u64) {
        tokio::time::advance(Duration::from_millis(10)).await;
        window
            .rate
            .observe(bytes_per_second / 100, Duration::from_millis(20));
    }

    /// Before the control law has moved the window, a re-grant returns exactly
    /// what was freed, and only once enough has been.
    #[test]
    fn a_grant_waits_for_the_threshold_then_returns_what_was_freed() {
        let mut window = Window::new(sizing(Growth::default(), 256 * 1024));
        window.on_payload(1024, Some(RTT));
        assert_eq!(
            window.take_grant(0, Some(RTT)),
            None,
            "granted before enough had been freed"
        );

        window.on_payload(64 * 1024, Some(RTT));
        assert_eq!(
            window.take_grant(0, Some(RTT)),
            Some(65 * 1024),
            "an unmoved window should hand back exactly what it freed"
        );
        assert_eq!(window.granted(), 256 * 1024, "the window moved on its own");
    }

    /// The backpressure that replaced refusing to read the socket. Withholding
    /// the grant leaves the connection readable, so the credit that unblocks
    /// it can still arrive.
    #[test]
    fn a_grant_is_withheld_while_the_application_is_behind() {
        let mut window = Window::new(sizing(Growth::default(), 256 * 1024));
        window.on_payload(64 * 1024, Some(RTT));
        assert_eq!(window.take_grant(256 * 1024, Some(RTT)), None);
        assert_eq!(
            window.take_grant(0, Some(RTT)),
            Some(64 * 1024),
            "the grant never arrived once the application caught up"
        );
    }

    /// A flat threshold means something different on every link; pacing holds
    /// the delay it adds to the credit loop to a fixed share of a round trip.
    #[tokio::test(start_paused = true)]
    async fn the_paced_threshold_tracks_the_link_within_its_bounds() {
        let mut paced = sizing(Growth::default(), 2 * 1024 * 1024);
        paced.re_grants_per_rtt = 32;

        let mut slow = Window::new(paced);
        assert_eq!(
            slow.re_grant_threshold(None),
            64 * 1024,
            "used something other than the flat threshold with no round trip"
        );

        // 10 Mbit: a round trip carries 50 KB, so a thirty-second of it is
        // well under the floor and the floor should win.
        at_rate(&mut slow, 1_250_000).await;
        assert_eq!(slow.re_grant_threshold(Some(RTT)), RE_GRANT_FLOOR);

        // 1 Gbit: a thirty-second of a round trip is over the flat cap, which
        // should win instead - this is the case a flat 64 KiB already served.
        let mut fast = Window::new(paced);
        at_rate(&mut fast, 125_000_000).await;
        assert_eq!(fast.re_grant_threshold(Some(RTT)), 64 * 1024);
    }

    #[tokio::test(start_paused = true)]
    async fn the_delay_rule_shrinks_on_a_queue_it_did_not_want() {
        let mut window = Window::new(sizing(ledbat(0.5), 256 * 1024));
        with_base(&mut window).await;

        window.on_delay(1_050_000);
        window.steer(Some(RTT));
        assert_eq!(
            window.desired(),
            128 * 1024,
            "a queue ten times the target did not shrink the window"
        );
    }

    /// Growth needs the peer to say it wanted the credit. Without that an idle
    /// direction - one that can only return what the other delivers - reads no
    /// queue and grows into the cap.
    #[tokio::test(start_paused = true)]
    async fn the_delay_rule_will_not_grow_a_window_nobody_is_filling() {
        let mut window = Window::new(sizing(ledbat(0.5), 256 * 1024));
        with_base(&mut window).await;

        window.on_delay(1_000_000);
        window.steer(Some(RTT));
        assert_eq!(window.desired(), 256 * 1024, "grew without being asked to");

        tokio::time::advance(RTT).await;
        window.on_delay(1_000_000);
        window.note_peer_starved();
        window.steer(Some(RTT));
        assert_eq!(
            window.desired(),
            384 * 1024,
            "did not grow for a peer that had run out of credit"
        );
    }

    /// Credit already granted is never taken back, so a window comes down at
    /// the pace freed credit allows and no faster.
    #[tokio::test(start_paused = true)]
    async fn a_shrink_is_limited_to_the_credit_just_freed() {
        let mut window = Window::new(sizing(ledbat(0.5), 256 * 1024));
        with_base(&mut window).await;

        window.on_delay(1_050_000);
        window.steer(Some(RTT));
        assert_eq!(window.desired(), 128 * 1024, "the target did not halve");

        // 64 KiB freed against a 128 KiB drop wanted: the window may only come
        // down by what it just freed, and grants nothing this time round.
        window.on_payload(64 * 1024, Some(RTT));
        assert_eq!(
            window.take_grant(0, Some(RTT)),
            None,
            "handed back credit while shrinking by everything it freed"
        );
        assert_eq!(window.granted(), 192 * 1024, "shrank by more than it freed");
    }
}
