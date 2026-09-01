//! State shared by all streams of a single rammux connection.

use std::{num::NonZeroU32, ops::Not, time::Duration};

use tokio::time::Instant;

use crate::{config::RammuxRole, probe::Probe};

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
            // Measure the arrival rate over buckets of at least
            // max(2 x RTT, 10ms) - rates sampled over shorter horizons
            // are burst artifacts, not throughput.
            recv.rate_bucket_bytes += u64::from(len);
            let bucket =
                (2 * rtt.unwrap_or(Duration::from_millis(5))).max(Duration::from_millis(10));
            let elapsed = recv.rate_bucket_start.elapsed();
            if elapsed >= bucket {
                let rate = recv.rate_bucket_bytes as f64 / elapsed.as_secs_f64();
                recv.rate_ema = if recv.rate_ema == 0.0 {
                    rate
                } else {
                    0.5 * recv.rate_ema + 0.5 * rate
                };
                recv.rate_bucket_bytes = 0;
                recv.rate_bucket_start = Instant::now();
            }
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
        let optimal = match self.rtt {
            Some(rtt) if recv.rate_ema > 0.0 && elapsed < 2 * rtt => {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let ceiling = (2.0 * rtt.as_secs_f64() * recv.rate_ema) as u32;
                recv.current
                    .saturating_mul(2)
                    .min(ceiling)
                    .max(recv.current)
            },
            _ => recv.current,
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
    /// Smoothed arrival rate, bytes per second.
    pub rate_ema: f64,
    /// Start of the current rate-measurement bucket.
    pub rate_bucket_start: Instant,
    /// Payload bytes received in the current rate-measurement bucket.
    pub rate_bucket_bytes: u64,
    /// Time of the last produced window update.
    pub last_update: Instant,
}

impl TransitRecv {
    pub fn new(initial: NonZeroU32, max: u32) -> Self {
        Self {
            current: initial.get(),
            freed: 0,
            max: max.max(initial.get()),
            rate_ema: 0.0,
            rate_bucket_start: Instant::now(),
            rate_bucket_bytes: 0,
            last_update: Instant::now(),
        }
    }

    /// Whether a `SESSION_WINDOW_UPDATE` is due.
    fn can_update(&self) -> bool {
        self.freed >= self.current / 2
    }
}
