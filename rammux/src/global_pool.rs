//! State shared by all streams of a single rammux connection.

use std::{num::NonZeroU32, ops::Not, time::Duration};

use tokio::time::Instant;

use crate::{
    config::{RammuxRole, TransitGrowth},
    probe::Probe,
    rate::{RateEstimate, rate_tau},
};

/// Global state shared by all rammux streams within a single rammux connection.
///
/// Used as the polling strategy of the connection's task selector.
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    ///
    /// Every sample comes from the link-clearing probe, so it is measured
    /// over a cooperatively drained link: the connection's own standing
    /// queue cannot inflate it.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
    /// The link-clearing probe and the plain ping: rammux's `PING`
    /// mechanisms and its two RTT estimators.
    pub probe: Probe,
    /// In-flight credit granted to us by the peer, if the transit window is enabled.
    pub transit_send: Option<TransitSend>,
    /// State of the transit window we grant to the peer, if enabled.
    pub transit_recv: Option<TransitRecv>,
    /// Latest loaded RTT: the round trip as it actually is with the
    /// connection's queues standing, rather than over the drained link.
    /// Stream receive windows size from this, because a stream's credit
    /// loop runs through those queues.
    pub dirty_rtt: Option<Duration>,
    /// How the transit window we grant grows.
    pub growth: TransitGrowth,
    /// Set by a stream whose outbound poll was held back solely by an
    /// exhausted transit window.
    ///
    /// Sticky: streams that returned pending on transit credit are not
    /// polled again until a grant wakes them, so the flag has to survive
    /// the passes in between. It is cleared when credit arrives and when
    /// payload is actually emitted.
    pub transit_blocked: bool,
    /// When the current transit stall began, if the sender is stalled.
    pub stalled_since: Option<Instant>,
    /// Total time the sender spent stalled on transit credit.
    pub stalled_total: Duration,
    /// How many transit stalls the sender has entered.
    pub stalled_events: u64,
}

impl Default for GlobalPool {
    fn default() -> Self {
        Self {
            rtt: None,
            available: 0,
            probe: Probe::new(RammuxRole::Client),
            transit_send: None,
            transit_recv: None,
            dirty_rtt: None,
            growth: TransitGrowth::default(),
            transit_blocked: false,
            stalled_since: None,
            stalled_total: Duration::ZERO,
            stalled_events: 0,
        }
    }
}

impl GlobalPool {
    /// Accounts for one completed pass of the outbound loop.
    ///
    /// `blocked` means the transport was ready to accept more frames and at
    /// least one stream had payload to write, but the transit window was
    /// spent. That is the only state in which the credit-return loop - not
    /// the link, the peer's stream windows, or the probe pause - is what
    /// holds the sender back. Time spent with a merely *fully utilised*
    /// transit window is not a stall: the link is busy draining it.
    pub fn note_outbound_pass(&mut self, blocked: bool) {
        match (blocked, self.stalled_since) {
            (true, None) => {
                self.stalled_since = Some(Instant::now());
                self.stalled_events += 1;
            },
            (false, Some(at)) => {
                self.stalled_total += at.elapsed();
                self.stalled_since = None;
            },
            _ => {},
        }
    }

    /// Returns whether outbound frames other than probe frames
    /// must be held back for the link-clearing probe.
    pub fn probe_paused(&self) -> bool {
        self.probe.paused()
    }

    /// Notifies the transit window that `len` bytes of `DATA` payload were received
    /// and stored in this muxer.
    pub fn transit_recv_freed(&mut self, len: usize) {
        let rtt = self.rtt;
        if let Some(recv) = self.transit_recv.as_mut() {
            let len = u32::try_from(len).unwrap_or(u32::MAX);
            recv.freed = recv.freed.saturating_add(len);
            // The first round trip after a grant still runs on the old
            // window - the grant has to reach the sender and its data has
            // to come back - so it would dilute the new size's rate with
            // the old one's. Left out of the measurement.
            if rtt.is_none_or(|rtt| recv.grew_at.elapsed() >= rtt) {
                recv.bytes_since_growth += u64::from(len);
            }
            recv.rate.observe(u64::from(len), rate_tau(rtt));
        }
    }

    /// Produces the next `SESSION_WINDOW_UPDATE` value, if one is due.
    pub fn transit_recv_update(&mut self) -> Option<u32> {
        let recv = self.transit_recv.as_mut()?;
        if recv.can_update().not() {
            return None;
        }

        // Growth needs evidence of window limitation, judged against the
        // clean-probe RTT - a yardstick our own queue cannot inflate.
        // The measured arrival rate is capped by the current window, so
        // it cannot size the next window directly (chicken-and-egg);
        // instead it bounds growth: the window doubles while limited,
        // ceilinged at 2 x rate x clean RTT, which stops growth at
        // about twice the loop BDP once the link is the bottleneck.
        // Grow-only.
        //
        // The ceiling stays a multiple of the *clean* BDP. Sizing it from
        // the loaded RTT runs away: a bigger window queues more, which
        // raises the loaded RTT, which raises the ceiling again.
        let elapsed = recv.last_update.elapsed();
        let optimal = match self.growth {
            TransitGrowth::RateCeiling => match self.rtt {
                Some(rtt) if recv.rate.get() > 0.0 && elapsed < 2 * rtt => {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let ceiling = (2.0 * rtt.as_secs_f64() * recv.rate.get()) as u32;
                    recv.current
                        .saturating_mul(2)
                        .min(ceiling)
                        .max(recv.current)
                },
                _ => recv.current,
            },
            // No "is the sender consuming" gate here, unlike the other two:
            // a starved sender on a long path also produces slow updates,
            // and the plateau test already refuses an idle one, since more
            // window buys it no more rate.
            TransitGrowth::RatePlateau => match self.rtt {
                Some(rtt) if recv.grew_at.elapsed() >= 4 * rtt => {
                    // Three round trips of clean measurement: the first is
                    // excluded above.
                    let measured = recv.grew_at.elapsed().saturating_sub(rtt);
                    let sustained = recv.bytes_since_growth as f64 / measured.as_secs_f64();
                    recv.bytes_since_growth = 0;
                    recv.grew_at = Instant::now();
                    if recv.warmup {
                        // The ramp, not the window. Discarded, so the next
                        // interval measures the initial size at rate and
                        // becomes the baseline the first step is judged
                        // against.
                        recv.warmup = false;
                        recv.current
                    } else if sustained >= TRANSIT_PLATEAU_GAIN * recv.baseline_rate {
                        // This size bought more than the last one did: the
                        // window is still what limits the sender. Step up,
                        // and measure the new size from here.
                        recv.baseline_rate = sustained;
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        let stepped = (f64::from(recv.current) * TRANSIT_PLATEAU_STEP) as u32;
                        stepped.max(recv.current.saturating_add(1))
                    } else {
                        // No gain over the last size. Hold, and measure again
                        // against the same baseline: a single four-round-trip
                        // average carries the sender's stall cadence and the
                        // path's jitter, and a step back on one reading was
                        // tried and stopped connections at a fifth of the
                        // pipe. A real plateau keeps failing this; a noisy one
                        // passes next time. Grow-only, like the other rules.
                        recv.current
                    }
                },
                _ => recv.current,
            },
        };
        let clamped = optimal.min(recv.max);

        let update = recv.freed + (clamped - recv.current);
        recv.current = clamped;
        recv.freed = 0;
        recv.last_update = Instant::now();

        (update > 0).then_some(update)
    }
}

/// Sender side of the transit window: how many more bytes of `DATA` payload
/// we may put in flight towards the peer.
pub struct TransitSend {
    pub credit: u32,
}

/// Receiver side of the transit window: tracks credit to return to the peer.
///
/// Credit is freed as soon as `DATA` payload is received and stored in this muxer,
/// independently of when the application reads it.
pub struct TransitRecv {
    /// Current window size.
    pub current: u32,
    /// Bytes freed since the last update.
    pub freed: u32,
    /// Autotune growth limit.
    pub max: u32,
    /// Smoothed arrival rate on the connection.
    pub rate: RateEstimate,
    /// Time of the last produced window update.
    pub last_update: Instant,
    /// How much freed credit is re-granted at once; see
    /// [`RammuxConfig::transit_update_threshold`](crate::config::RammuxConfig::transit_update_threshold).
    pub update_threshold: NonZeroU32,
    /// Inbound rate the previous window size sustained, bytes per second, for
    /// [`TransitGrowth::RatePlateau`]. Zero until the first decision.
    pub baseline_rate: f64,
    /// Whether the first measurement interval is still to be discarded.
    ///
    /// That interval starts when the connection does, so it catches the
    /// transport's own ramp rather than what the window sustains: the rate it
    /// reports is low, the next interval beats it whatever the window did,
    /// and the rule takes a step it has no evidence for. On a link whose
    /// bandwidth-delay product is below the initial window that step is the
    /// entire error - measured at 10 Mbit/s over 40 ms, discarding it is
    /// worth 158 ms of echo latency.
    pub warmup: bool,
    /// Payload bytes received since the window last grew.
    pub bytes_since_growth: u64,
    /// When the window last grew (or was created). Together with
    /// `bytes_since_growth` this gives the rate the current size actually
    /// sustained, as an exact interval average rather than the smoothed EMA,
    /// which is still half way to the new rate when a decision falls due.
    pub grew_at: Instant,
}

/// How much a growth step has to raise the inbound rate for the next one to
/// be allowed, under [`TransitGrowth::RatePlateau`].
///
/// A window-limited connection gains the full step ratio; a link-limited one
/// gains nothing. A quarter sits between the two with room for the noise of
/// a four-round-trip average.
pub const TRANSIT_PLATEAU_GAIN: f64 = 1.25;

/// The growth step under [`TransitGrowth::RatePlateau`].
///
/// Not a doubling. The step that discovers the plateau is the one past it,
/// so the window ends up one step above the first size that reached the
/// link; with a doubling that is up to twice the pipe's worth of standing
/// queue, with this it is at most half again. Nine steps from 128 KiB to
/// 3 MiB rather than five, at four round trips each.
pub const TRANSIT_PLATEAU_STEP: f64 = 1.5;

impl TransitRecv {
    pub fn new(initial: NonZeroU32, max: u32, update_threshold: NonZeroU32) -> Self {
        Self {
            update_threshold,
            current: initial.get(),
            freed: 0,
            max: max.max(initial.get()),
            rate: RateEstimate::default(),
            last_update: Instant::now(),
            baseline_rate: 0.0,
            warmup: true,
            bytes_since_growth: 0,
            grew_at: Instant::now(),
        }
    }

    /// Whether a `SESSION_WINDOW_UPDATE` is due.
    fn can_update(&self) -> bool {
        self.freed >= self.update_threshold.get().min(self.current / 2)
    }
}
