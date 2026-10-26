//! The connection window of `TransitControl::CopaLike`, see `TRANSIT_CONTROL.md`.
//!
//! Copa-style: the connection keeps about `alpha` bytes in the queue of the bottleneck, and every
//! `PONG` moves its window `W` up or down by its share of a step of `alpha` per round trip (Copa's
//! per-ACK update). Everything here is driven by `PONG`s.
//!
//! One law with two states, `(W, g)`: `W` is the window, and the growth `g`
//! is how many bytes the window grows by per round trip while the queue is below `alpha`. `g` is
//! `alpha` (Copa's step), or doubles per round trip while there is evidence that the growth buys
//! throughput: startup is that growth from the first round trip on, and so is the recovery after
//! room appears. Both end the same way, see [`DelayControl::on_pong`].
//! Time is passed in as tokio `Instant`s and wire timestamps, so the whole thing can run on a paused clock.

use std::{collections::VecDeque, task::Context, time::Duration};

use tokio::time::Instant;

use crate::{
    clock::diff,
    pacer::Pacer,
    transit::{RateEstimate, SRTT_MIN},
};

/// How many round trips of raw one-way delay samples the re-baseline test looks at.
///
/// The test is sound only if the connection's own queue gets nearly empty within this history, so it
/// must span several swings of the window (the queue swings around `alpha` about once or twice per
/// round trip). It is also how long a wrong base delay survives after a route change, so it cannot be
/// long. Five is a compromise between the two, not a derived value. (Copa uses a fixed 10 s window.)
const HISTORY_SRTT: u32 = 5;

/// How many of the newest samples the fast stop of the growth looks at: the 8 samples of HyStart
/// (Ha and Rhee, the slow start exit of CUBIC), which guard against a single delayed sample.
///
/// Coupling: the time span of these 8 samples is `8 * min(ping_bytes, W / 4) / R`, so `ping_bytes`,
/// which looks like a cadence setting, also sets how fast this test is and how jitter-proof (about 14
/// ms at 150 Mbit/s with the default spacing, 2 round trips at `W` = 4 frames), and the number of
/// samples in the standing minimum of `srtt / 2` (about 2 at small `W`). Change the `PING` spacing and
/// both move.
const RECENT_SAMPLES: usize = 8;

/// The rate estimate needs this many smoothed round trips of deliveries before its rise means
/// anything, see [`DelayControl::update_window`].
const RATE_WARMUP_SRTT: u32 = 2;

/// The pacing rate is this many windows per smoothed round trip, as in Copa.
const PACING_GAIN: f64 = 2.0;

/// A sliding window minimum or maximum of wrapping timestamps, in O(1) per sample.
///
/// The samples are compared with [`diff`]: they are only comparable when they are within about
/// 35 minutes of each other, which the windows guarantee.
struct WindowExtreme {
    samples: VecDeque<(Instant, u32)>,
    /// Whether this is the minimum, or the maximum.
    min: bool,
}

impl WindowExtreme {
    fn new(min: bool) -> Self {
        Self {
            samples: VecDeque::new(),
            min,
        }
    }

    fn push(&mut self, at: Instant, value: u32) {
        let min = self.min;
        while self.samples.back().is_some_and(|(_, back)| {
            let d = diff(*back, value);
            if min { d >= 0 } else { d <= 0 }
        }) {
            self.samples.pop_back();
        }
        self.samples.push_back((at, value));
    }

    /// Forgets the samples older than the window that ends at `now`.
    fn expire(&mut self, now: Instant, window: Duration) {
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > window)
        {
            self.samples.pop_front();
        }
    }

    fn get(&self) -> Option<u32> {
        self.samples.front().map(|(_, value)| *value)
    }
}

/// The one-way delay of one direction: its minimum (the base delay), the queue above it,
/// and the test that raises the minimum when it is stale.
///
/// A sample is the difference of two clocks, the real delay plus an unknown, nearly constant
/// offset, so only differences between samples mean anything.
struct OwdTracker {
    /// The base delay, `owd_min`. A lower sample lowers it at once;
    /// it rises only in [`Self::rebaseline`].
    floor: Option<u32>,
    /// The smallest raw sample of the last 5 round trips, and the largest standing value.
    ///
    /// The queue of each sample against the *current* floor is `sample - floor`, so the minimum
    /// and the maximum of the window are all that the re-baseline test needs. Test it against the
    /// samples and the floor, never against queues computed back when the floor was another. The
    /// peak is taken from the standing values: a spike of jitter is not a queue that came and went.
    history_min: WindowExtreme,
    history_max: WindowExtreme,
    /// The smallest sample of the last `srtt / 2`, the newest included: Copa's "standing" delay.
    /// It filters out jitter and bunching but keeps a queue that persists. (Coupling: it holds
    /// `PING` samples, so how many of them it sees depends on `ping_bytes`, see `RECENT_SAMPLES`.)
    standing: WindowExtreme,
    /// When the history started to count: the first sample, or the last re-baseline. The test
    /// needs a whole history of 5 round trips since then. Without it, a connection that is still
    /// starting takes the queue of its startup for the delay of the path, and a re-baseline
    /// repeats as soon as the sample it was based on leaves the window.
    epoch: Option<Instant>,
    rebaselines: u64,
}

impl OwdTracker {
    fn new() -> Self {
        Self {
            floor: None,
            history_min: WindowExtreme::new(true),
            history_max: WindowExtreme::new(false),
            standing: WindowExtreme::new(true),
            epoch: None,
            rebaselines: 0,
        }
    }

    fn push(&mut self, now: Instant, sample: u32, srtt: Duration) {
        self.epoch.get_or_insert(now);
        self.floor = Some(match self.floor {
            Some(floor) if diff(sample, floor) >= 0 => floor,
            _ => sample,
        });
        self.history_min.push(now, sample);
        self.standing.push(now, sample);
        self.standing.expire(now, srtt / 2);
        if let Some(standing) = self.standing.get() {
            self.history_max.push(now, standing);
        }
        let history = srtt * HISTORY_SRTT;
        self.history_min.expire(now, history);
        self.history_max.expire(now, history);
    }

    /// The queue now, in microseconds: the standing delay above the base delay.
    fn queue(&self) -> u32 {
        match (self.standing.get(), self.floor) {
            (Some(standing), Some(floor)) => micros_above(standing, floor),
            _ => 0,
        }
    }

    /// The test: if no sample of the last 5 round trips had a nearly empty queue, the base delay
    /// is not the delay of the path any more. Raises it to the smallest of those samples.
    fn rebaseline(&mut self, now: Instant, srtt: Duration) {
        let (Some(floor), Some(lowest), Some(highest), Some(epoch)) = (
            self.floor,
            self.history_min.get(),
            self.history_max.get(),
            self.epoch,
        ) else {
            return;
        };
        if now.saturating_duration_since(epoch) < srtt * HISTORY_SRTT {
            return;
        }
        let smallest = micros_above(lowest, floor);
        let peak = micros_above(highest, floor);
        // Some sample had a queue of at most a quarter of the standing peak. Copa uses 10% of the
        // largest raw sample, which assumes that the queue empties completely. Two connections of ours
        // with different round trips swing out of phase and do not empty their joint queue together,
        // so with 10% each takes part of the other's queue for the path, and the base delay ratchets
        // up. A quarter of the standing peak (the standing value keeps a spike of jitter out of the
        // peak) tolerates the part of the joint queue that never drains, and still removes a phantom
        // larger than a third of the swing of our own queue (per-packet jitter, a rise of the path delay).
        let nearly_empty = u64::from(smallest) * 4 <= u64::from(peak);
        if !nearly_empty {
            self.floor = Some(lowest);
            self.epoch = Some(now);
            self.rebaselines += 1;
        }
    }
}

/// `a - b` in microseconds, or 0 if `a` is not above `b`.
fn micros_above(a: u32, b: u32) -> u32 {
    u32::try_from(diff(a, b).max(0)).unwrap_or(0)
}

fn secs(micros: u32) -> f64 {
    f64::from(micros) / 1e6
}

/// What a `PONG` says, with the time we received it.
#[derive(Debug, Clone, Copy)]
pub struct PongSample {
    /// When the header of the `PONG` was read.
    pub now: Instant,
    /// Our clock at that moment, as on the wire.
    pub now_us: u32,
    pub t_echo: u32,
    pub t_read: u32,
    pub rx_bytes: u32,
}

/// Measurements and the window of a connection with `CopaLike`.
pub struct DelayControl {
    /// `alpha`, the bytes to keep in the queue.
    alpha: f64,
    frame_limit: u64,
    /// The smallest window, and the start: four frames. `W / 4` is then a frame at least, so the
    /// `PING` spacing of `W / 4` bytes always leaves room for one.
    min_window: u64,

    /// The window `W`: the bytes that may be in flight on the forward path.
    window: u64,
    /// The growth `g`: the bytes the window grows by per round trip while `own < alpha` and the
    /// window is what limits sending. At least `alpha`; more only on evidence (see
    /// [`Self::update_window`]), and back to `alpha` the moment the queue reaches `alpha`.
    growth: u64,
    /// When the window last kept a frame from being sent. "Limited" means one round trip at most
    /// ago ([`Self::limited`]): the one meaning, wherever the connection asks.
    limited_at: Option<Instant>,
    /// Our clock at the last update of the window. The next one needs a `PONG` of a later `PING`.
    last_update_us: Option<u32>,
    /// Whether every `PONG` since the last update found `own < alpha / 2`.
    all_small: bool,
    /// The delivery rate at the last update.
    rate_prev: f64,
    /// When the first delivery was reported: the estimate of the rate has no history before.
    rate_since: Option<Instant>,
    /// Whether to send a `PING` now: sending is blocked by the window, and the `PONG` that
    /// would free it is not on the way.
    ping_wanted: bool,

    out: OwdTracker,
    back: OwdTracker,
    /// The delivery rate, from `rx_bytes` of the `PONG`s.
    rate: RateEstimate,
    /// The newest `rx_bytes` the peer reported: `peer_rx`. `PONG`s arrive in order, and a counter
    /// that does not move forward is not a delivery, so this is also the baseline of the next one.
    peer_rx: Option<u32>,
    srtt: Duration,

    q_fwd: u32,
    q_back: u32,
    own: f64,

    /// The newest raw `owd_out` samples, for the startup exit test.
    recent_out: VecDeque<u32>,

    /// The token bucket and the timer of the pacer, at the rate of [`Self::pace_rate`].
    pacer: Pacer,
}

/// A snapshot of the measurements and the window of a connection with
/// [`TransitControl::CopaLike`](crate::config::TransitControl::CopaLike).
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct CopaLikeStats {
    /// The queue on our forward path, from the one-way delay of `PING`s.
    pub q_fwd: Duration,
    /// How long the reports of the peer currently wait on their way back to us.
    pub q_back: Duration,
    /// Bytes of ours in the queue of the bottleneck, `rate * q_fwd`.
    pub own: u64,
    /// The delivery rate, in bytes per second, from the bytes the peer reports.
    pub rate: f64,
    /// Bytes on the forward path, as the connection window counts them.
    pub fwd_in_flight: u64,
    /// The connection window, in bytes.
    pub window: u64,
    /// How many times the base delay of the forward path was raised.
    pub rebaselines_fwd: u64,
    /// How many times the base delay of the way back was raised.
    pub rebaselines_back: u64,
}

impl DelayControl {
    pub fn new(alpha: u32, frame_limit: u16) -> Self {
        let frame_limit = u64::from(frame_limit);
        let now = Instant::now();
        Self {
            alpha: f64::from(alpha),
            frame_limit,
            window: 4 * frame_limit,
            min_window: 4 * frame_limit,
            // The start: as much as the window, `alpha` at least (the same at the defaults).
            growth: (4 * frame_limit).max(u64::from(alpha)),
            limited_at: None,
            last_update_us: None,
            all_small: true,
            rate_prev: 0.0,
            rate_since: None,
            ping_wanted: false,
            out: OwdTracker::new(),
            back: OwdTracker::new(),
            rate: RateEstimate::new(now),
            peer_rx: None,
            srtt: SRTT_MIN,
            q_fwd: 0,
            q_back: 0,
            own: 0.0,
            recent_out: VecDeque::new(),
            pacer: Pacer::new(now),
        }
    }

    /// Takes a `PONG` into account. Call it after `srtt` was updated; `tx_bytes` is what we have sent.
    ///
    /// The law, per `PONG` with `delivered` new bytes and the share
    /// `s = min(delivered / W, 1)` of a round trip:
    ///
    /// ```text
    /// stop = max(own, own8) >= alpha      own8: the same on the newest 8 raw samples (HyStart)
    /// if stop and g > alpha:  W = min(W, fwd_in_flight), g = alpha
    /// if own <  alpha and limited:  W += g * s
    /// if own >= alpha:              W -= (alpha + max(0, own - 2 alpha)) * s
    /// ```
    ///
    /// Startup is not a mode: it is `g` growing from the first round trip. A growth that is not
    /// `alpha` is geometric, so it overshoots by what the delay of the measurement hides, about a
    /// doubling; the stop keeps only what is on the path. That is HyStart's exit with the clamp of
    /// `W` to the bytes in flight (the unsent part of a doubled window would only queue), and it is
    /// the same whether the growth is the first one or the recovery after room appeared. `own8` is the
    /// faster of the two detectors at any bandwidth-delay product: the standing minimum of `srtt / 2`
    /// is the more robust, the newest 8 samples the more prompt.
    pub fn on_pong(&mut self, pong: PongSample, srtt: Duration, tx_bytes: u32) {
        self.srtt = srtt;
        let srtt_s = srtt.as_secs_f64();

        // The delivery rate, from the bytes that the peer had received when it read our PING.
        // Wrapping arithmetic, and a counter that does not move forward is not a delivery.
        let mut delivered_now = 0;
        match self.peer_rx {
            None => self.peer_rx = Some(pong.rx_bytes),
            Some(prev) => {
                let delivered = u32::try_from(diff(pong.rx_bytes, prev).max(0)).unwrap_or(0);
                delivered_now = delivered;
                if delivered > 0 {
                    self.rate.add(delivered, pong.now, srtt_s);
                    self.rate_since.get_or_insert(pong.now);
                    self.peer_rx = Some(pong.rx_bytes);
                }
            },
        }
        let rate = self.rate.value(pong.now, srtt_s);

        // One-way delays, with offsets that cancel in the queues.
        let owd_out = pong.t_read.wrapping_sub(pong.t_echo);
        if self.recent_out.len() == RECENT_SAMPLES {
            self.recent_out.pop_front();
        }
        self.recent_out.push_back(owd_out);
        self.out.push(pong.now, owd_out, srtt);
        self.back
            .push(pong.now, pong.now_us.wrapping_sub(pong.t_read), srtt);
        self.out.rebaseline(pong.now, srtt);
        self.back.rebaseline(pong.now, srtt);
        self.q_fwd = self.out.queue();
        self.q_back = self.back.queue();
        self.own = rate * secs(self.q_fwd);
        let own8 = rate * self.newest_queue();

        // The growth needs an empty queue all round trip (half of `alpha` at most), and ends the
        // moment the queue reaches `alpha`, not a round trip later.
        // Half of `alpha` is the midpoint between an empty queue and the target: a doubling adds up to
        // a window of bytes, so growing geometrically while the queue already holds half the target
        // would overshoot it within the round trip. A tuning choice, not a derived value.
        if self.own >= self.alpha / 2.0 {
            self.all_small = false;
        }
        if self.own.max(own8) >= self.alpha && self.growth > to_bytes(self.alpha) {
            let on_path = self.fwd_in_flight(pong.now, tx_bytes);
            self.window = self.window.min(on_path).max(self.min_window);
            self.growth = to_bytes(self.alpha);
        }
        self.step(delivered_now, pong.now);

        // One update per round trip: when the PONG answers a PING sent after the last update.
        if self
            .last_update_us
            .is_none_or(|updated| diff(pong.t_echo, updated) > 0)
        {
            self.last_update_us = Some(pong.now_us);
            self.update_window(rate, pong.now);
        }
    }

    /// The queue of the smallest of the newest `RECENT_SAMPLES` raw samples above the base delay,
    /// in seconds: 0 until there are that many. HyStart's 8 samples, against jitter.
    fn newest_queue(&self) -> f64 {
        if self.recent_out.len() < RECENT_SAMPLES {
            return 0.0;
        }
        let lowest = self
            .recent_out
            .iter()
            .copied()
            .reduce(|a, b| if diff(a, b) <= 0 { a } else { b });
        match (lowest, self.out.floor) {
            (Some(lowest), Some(floor)) => secs(micros_above(lowest, floor)),
            _ => 0.0,
        }
    }

    /// Whether the window is what limits sending: it kept a frame from being sent within the last
    /// smoothed round trip. The one meaning, for the steps and for the growth alike. (A `PONG` early
    /// in a round trip comes before any frame of it could be blocked; the look back of a whole round
    /// trip covers that, and an app-limited connection is not limited for longer than that.)
    fn limited(&self, now: Instant) -> bool {
        self.limited_at
            .is_some_and(|at| now.saturating_duration_since(at) <= self.srtt)
    }

    /// The steps of `alpha` per round trip, spread over its `PONG`s (Copa's per-ACK update): each
    /// `PONG` moves the window by its share of the round trip, `delivered / W` (at most 1), in the
    /// direction its own measurement says. Below `alpha` queued (and the connection limited): up by
    /// `g`, which is `alpha` unless the growth is geometric. Above: down by `alpha`, or by the excess
    /// over `alpha` if that is more.
    ///
    /// Why the excess too, and spread over the PONGs: a once-per-round-trip cut of `own - alpha` would
    /// take the measurement of one `PONG`, and on a jittery path that one may be a spike. Spread, the
    /// cut follows the mean of the round trip's measurements, and a capacity drop or the overshoot of
    /// startup is still shed within a round trip.
    ///
    /// Why not one step per round trip: connections that share a bottleneck see the same queue, and
    /// the queue swings by `2 alpha / C` per round trip with their steps, more than the band
    /// `alpha / R1 .. alpha / R2` in which only the faster one would step down. So both step the same
    /// way at nearly every update, and which of them sees the higher queue at its decreases is set by
    /// the fixed phase of its updates in the swing: the split between two connections freezes at
    /// whatever that phase gives, not at an even one. Spread over the round trip, a connection steps
    /// down for as long as the queue is in that band, and the faster one is there longer: the shares
    /// converge.
    fn step(&mut self, delivered: u32, now: Instant) {
        if delivered == 0 {
            return;
        }
        // At most one step of `alpha` per PONG: a clump of reports after a stall of the reverse
        // direction can report far more than a window at once.
        let share = (f64::from(delivered) / self.window.max(1) as f64).min(1.0);
        if self.own < self.alpha {
            if self.limited(now) {
                self.window = self
                    .window
                    .saturating_add(to_bytes(self.growth as f64 * share));
            }
        } else {
            // The excess cut: the excess over `alpha` is shed in about one round trip.
            // INVARIANT: its gain of 1 is load-bearing. The `own` it reads is a round trip old, so by
            // the time the cut is applied the queue has fallen, and the cut (normalised by `W`, not
            // by the old round trip) overshoots by about `1 + q / d`. That overcut is what lets the
            // queue touch nearly empty within each re-baseline history, and so what lets the
            // re-baseline test see an empty queue (`nearly_empty`) instead of taking our own queue
            // for the path. A gain of 1/2, a Smith predictor or a share normalised by the sample's
            // round trip leave the queue standing, and the base delay creeps up. Change nothing here
            // without the long-run tests (`the_base_delay_does_not_ratchet...`,
            // `the_floor_does_not_creep_at_low_bdp`, `the_excess_cut_overshoots_so_a_capacity_drop_is_shed_fast`).
            let step = self.alpha.max(self.own - self.alpha);
            self.window = self
                .window
                .saturating_sub(to_bytes(step * share))
                .max(self.min_window);
        }
    }

    /// Once per round trip: the growth for the next one. There is no idle handling:
    /// a window that was not used is kept, and does not grow (`limited`).
    ///
    /// `g` doubles, but never beyond `W` (at most a doubling of the window per round trip), and only
    /// if the growth is buying throughput: the queue stayed empty all round trip (`all_small`), `W`
    /// limited sending, and the rate rose by at least half of one step of `alpha`, `alpha / srtt`.
    /// Otherwise `g` is `alpha`. A rate that rises while the queue stays empty means that the window
    /// was the limit; a flat rate means the link, or the TCP below it, is.
    ///
    /// The threshold does not depend on `g`. The rate (a time constant of `srtt`,
    /// fed by reports a round trip old) lags the window by 2-3 round trips, so the rise it shows here
    /// is the step of 2-3 updates back, a quarter of the present one while `g` doubles: a threshold of
    /// `g / 2` would fail at the third doubling by construction (the 1-2-4 sawtooth, a mean step of
    /// 2.33 alpha). Half a minimum step is what a rise must exceed to be told from noise.
    ///
    /// Coupling: that lag is also this test's safety. The overshoot of a growth that the TCP below
    /// answers with a send buffer, not with a rate, is bounded by it: the rate does not rise, `g`
    /// returns to `alpha` within a round trip. Change the time constant of the rate, or the cadence
    /// of the `PONG`s, and the test becomes either useless (a lag over 3 round trips) or the 6 MB
    /// overshoot of a rate estimate with no lag.
    ///
    /// The estimator-validity rule: the rate has no history in its first two round trips, so its
    /// rise says nothing (it warms up from 0, and the test would fail the first doublings and slow
    /// down the start). That is a property of the estimate, not a mode: `rose` is passed while the
    /// rate has less than `RATE_WARMUP_SRTT` smoothed round trips of deliveries.
    fn update_window(&mut self, rate: f64, now: Instant) {
        let alpha = to_bytes(self.alpha);
        let limited = self.limited(now);
        let warm = self.rate_since.is_some_and(|since| {
            now.saturating_duration_since(since) >= self.srtt * RATE_WARMUP_SRTT
        });
        let rose = !warm || rate - self.rate_prev >= 0.5 * self.alpha / self.srtt.as_secs_f64();
        self.growth = if self.all_small && limited && rose {
            self.growth.saturating_mul(2).min(self.window).max(alpha)
        } else {
            alpha
        };
        self.all_small = true;
        self.rate_prev = rate;
    }

    /// The bytes that are on the forward path: in transit, minus those that are delivered
    /// but whose report is still on its way back.
    ///
    /// With bulk in both directions the reports queue behind the peer's data, and arrive in a
    /// lump when its queue drains. Counting them as in flight until then would release a burst
    /// of megabytes. What is subtracted is the rate times how long the reports currently wait on
    /// the way back (`q_back`). Nothing grows with the time since the last report: a stall of the
    /// forward path has no reports, and must not open the window. (Bursts are the pacer's business.)
    ///
    /// Coupling: the window excludes `R * q_back`, but the pacer's `srtt` includes `q_back`. Once
    /// `q_back > RTT_base + q_fwd` the pace `2W / srtt` falls below `R`, only the window counts as
    /// limited (the pacer never drives growth), and `W` stops growing while the pacer binds. The gain
    /// of 2 is the margin that hides this in steady state (a gain near 1.25 does not hide it when a
    /// competing upload fills the reverse queue); a reverse queue beyond a round trip is what the
    /// margin does not cover.
    pub fn fwd_in_flight(&self, now: Instant, tx_bytes: u32) -> u64 {
        let in_transit =
            u64::try_from(diff(tx_bytes, self.peer_rx.unwrap_or(0)).max(0)).unwrap_or(0);
        let reported = self.rate.value(now, self.srtt.as_secs_f64()) * secs(self.q_back);
        in_transit.saturating_sub(to_bytes(reported))
    }

    /// The room in the window: `W - fwd_in_flight`.
    pub fn room(&self, now: Instant, tx_bytes: u32) -> u64 {
        self.window
            .saturating_sub(self.fwd_in_flight(now, tx_bytes))
    }

    /// The pacing rate, in bytes per second: `PACING_GAIN * W / srtt`, as in Copa.
    ///
    /// Coupling: `srtt` includes `q_back`, and `W` (through [`Self::fwd_in_flight`]) excludes
    /// `R * q_back`. The gain of 2 is the margin between them, see there.
    fn pace_rate(&self) -> f64 {
        PACING_GAIN * self.window as f64 / self.srtt.as_secs_f64()
    }

    /// The room the pacer leaves, see [`Pacer::room`].
    pub fn pace_room(&self, now: Instant) -> u64 {
        self.pacer.room(now, self.pace_rate(), self.frame_limit)
    }

    /// Takes `n` bytes of DATA that were sent out of the pacer's room.
    pub fn on_sent(&mut self, now: Instant, n: u64) {
        self.pacer.take(now, n, self.pace_rate(), self.frame_limit);
    }

    /// Records that the pacer kept `wanted` bytes from being sent: they fit at the deadline.
    pub fn note_paced(&mut self, now: Instant, wanted: u64) {
        self.pacer
            .hold(now, wanted, self.pace_rate(), self.frame_limit);
    }

    /// Arms the timer of the pacer if it kept a frame back, and polls it: [`true`] if it fired.
    pub fn poll_pacer(&mut self, cx: &mut Context<'_>) -> bool {
        self.pacer.poll_timer(cx)
    }

    /// Records that the window kept something from being sent.
    pub fn note_limited(&mut self, now: Instant) {
        self.limited_at = Some(now);
        self.ping_wanted = true;
    }

    /// Takes the request for a `PING`: the window blocks sending, and a fresh sample may free it.
    pub fn take_ping_wanted(&mut self) -> bool {
        std::mem::take(&mut self.ping_wanted)
    }

    pub fn peek_ping_wanted(&self) -> bool {
        self.ping_wanted
    }

    /// The window.
    pub fn window(&self) -> u64 {
        self.window
    }

    pub fn stats(&self, now: Instant, tx_bytes: u32) -> CopaLikeStats {
        CopaLikeStats {
            q_fwd: Duration::from_micros(u64::from(self.q_fwd)),
            q_back: Duration::from_micros(u64::from(self.q_back)),
            own: to_bytes(self.own),
            rate: self.rate.value(now, self.srtt.as_secs_f64()),
            fwd_in_flight: self.fwd_in_flight(now, tx_bytes),
            window: self.window,
            rebaselines_fwd: self.out.rebaselines,
            rebaselines_back: self.back.rebaselines,
        }
    }

    #[cfg(test)]
    pub fn floors(&self) -> (Option<u32>, Option<u32>) {
        (self.out.floor, self.back.floor)
    }

    #[cfg(test)]
    pub fn queue_micros(&self) -> (u32, u32) {
        (self.q_fwd, self.q_back)
    }

    #[cfg(test)]
    pub fn own(&self) -> f64 {
        self.own
    }

    #[cfg(test)]
    pub fn set_window(&mut self, window: u64) {
        self.window = window;
    }

    #[cfg(test)]
    pub fn growth(&self) -> u64 {
        self.growth
    }

    #[cfg(test)]
    pub fn set_growth(&mut self, growth: u64) {
        self.growth = growth;
    }
}

/// A non-negative number of bytes, rounded down.
pub(crate) fn to_bytes(value: f64) -> u64 {
    if value.is_nan() || value <= 0.0 {
        0
    } else {
        // Saturates for values beyond u64.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            value as u64
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::delay::{DelayControl, PongSample};

    const FRAME: u16 = 16 * 1024;
    const ALPHA: u32 = 64 * 1024;
    const SRTT: Duration = Duration::from_millis(100);

    /// Feeds `PONG`s to a controller. The clocks have offsets, as the real ones do.
    struct Feed {
        dc: DelayControl,
        start: Instant,
        /// Our clock when `start` happened.
        us: u32,
        /// How much the peer's clock is ahead of ours.
        offset: u32,
        rx: u32,
        /// What we have sent and the peer has not reported: a lot, unless a test sets it.
        in_transit: u32,
    }

    impl Feed {
        fn new(offset: u32) -> Self {
            Self {
                dc: DelayControl::new(ALPHA, FRAME),
                start: Instant::now(),
                us: 5,
                offset,
                rx: 0,
                in_transit: 1 << 30,
            }
        }

        /// The window kept a frame from being sent, `at` after the start.
        fn limit(&mut self, at: Duration) {
            self.dc.note_limited(self.start + at);
        }

        /// A `PONG` that arrives `at` after the start, for a `PING` sent `rtt` earlier, whose
        /// trip there took `out`. `delivered` is what the peer received since the last `PONG`.
        fn pong(&mut self, at: Duration, rtt: Duration, out: Duration, delivered: u32) {
            self.pong_ago(at, rtt, out, delivered);
        }

        /// Like [`Self::pong`], with the `PING` sent `ago` before. The way back takes the rest.
        fn pong_ago(&mut self, at: Duration, ago: Duration, out: Duration, delivered: u32) {
            self.rx = self.rx.wrapping_add(delivered);
            let now_us = self.us.wrapping_add(micros(at));
            let t_echo = now_us.wrapping_sub(micros(ago));
            let t_read = t_echo.wrapping_add(micros(out)).wrapping_add(self.offset);
            self.dc.on_pong(
                PongSample {
                    now: self.start + at,
                    now_us,
                    t_echo,
                    t_read,
                    rx_bytes: self.rx,
                },
                SRTT,
                self.rx.wrapping_add(self.in_transit),
            );
        }
    }

    fn micros(d: Duration) -> u32 {
        u32::try_from(d.as_micros()).unwrap()
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn offsets_cancel_and_wrapping_is_harmless() {
        // The same delays under different offsets of the peer's clock, some of which make the
        // samples wrap around: the queues are identical.
        let offsets = [0, 1_000_000, 4_294_000_000, 2_147_483_647, 3_000_000_000];
        let mut feeds = offsets.map(Feed::new);
        for i in 0..200_u64 {
            // The queue swings between 0 and 40 ms.
            let queue = ms((i % 20) * 2);
            for feed in &mut feeds {
                feed.pong(ms(10 * i), SRTT + queue, ms(50) + queue, 10_000);
            }
            let reference = feeds[0].dc.queue_micros();
            for feed in &feeds {
                assert_eq!(feed.dc.queue_micros(), reference, "step {i}");
            }
        }
    }

    #[test]
    fn a_lower_sample_lowers_the_base_delay_at_once() {
        let mut feed = Feed::new(0);
        for i in 0..20 {
            feed.pong(ms(10 * i), ms(120), ms(70), 1000);
        }
        // All the samples are equal: there is no queue.
        assert_eq!(feed.dc.queue_micros().0, 0);
        // A sample 30 ms lower.
        feed.pong(ms(200), ms(90), ms(40), 1000);
        let (floor, _) = feed.dc.floors();
        assert_eq!(floor, Some(40_000_u32));
        assert_eq!(feed.dc.queue_micros().0, 0);
        // And the following, back at 70 ms, are a queue of 30 ms, once the standing window
        // (srtt / 2) has forgotten the low one.
        for i in 0..20 {
            feed.pong(ms(210 + 10 * i), ms(120), ms(70), 1000);
        }
        assert_eq!(feed.dc.queue_micros().0, 30_000);
    }

    #[test]
    fn the_standing_queue_ignores_a_spike() {
        let mut feed = Feed::new(0);
        for i in 0..30 {
            feed.pong(ms(10 * i), ms(100), ms(50), 1000);
        }
        // One late PONG among the others: the standing minimum (srtt / 2 = 50 ms) filters it out.
        feed.pong(ms(300), ms(180), ms(130), 1000);
        assert_eq!(feed.dc.queue_micros().0, 0);
        feed.pong(ms(305), ms(100), ms(50), 1000);
        assert_eq!(feed.dc.queue_micros().0, 0);
    }

    #[test]
    fn a_queue_that_never_empties_is_taken_for_the_path_once() {
        let mut feed = Feed::new(0);
        // The path is 50 ms, a first sample finds it so, then a constant 20 ms queue appears
        // that never empties: for example a route change.
        feed.pong(ms(0), ms(100), ms(50), 1000);
        for i in 1..=100 {
            feed.pong(ms(10 * i), ms(120), ms(70), 1000);
        }
        // Within 5 round trips (500 ms) the base delay moved up, and the queue is gone.
        let (floor, _) = feed.dc.floors();
        assert_eq!(floor, Some(70_000));
        assert_eq!(feed.dc.queue_micros().0, 0);
        // The test is applied to queues against the current base: it did not fire again on
        // every following PONG.
        let stats = feed.dc.stats(feed.start, 0);
        assert_eq!((stats.rebaselines_fwd, stats.rebaselines_back), (1, 0));
    }

    #[test]
    fn a_queue_that_empties_is_ours() {
        let mut feed = Feed::new(0);
        for i in 0..400_u64 {
            // A sawtooth of the queue, 0 to 38 ms every 20 PONGs, touching zero.
            let queue = ms((i % 20) * 2);
            feed.pong(ms(10 * i), SRTT + queue, ms(50) + queue, 1000);
        }
        let stats = feed.dc.stats(feed.start, 0);
        assert_eq!((stats.rebaselines_fwd, stats.rebaselines_back), (0, 0));
        assert_eq!(feed.dc.floors().0, Some(50_000));
    }

    #[test]
    fn the_back_direction_has_its_own_base_delay() {
        let mut feed = Feed::new(0);
        // PONGs that wait 30 ms behind the peer's data on their way back.
        for i in 0..20 {
            feed.pong(ms(10 * i), ms(100), ms(50), 1000);
        }
        for i in 20..40 {
            feed.pong(ms(10 * i), ms(130), ms(50), 1000);
        }
        let (q_fwd, q_back) = feed.dc.queue_micros();
        assert_eq!((q_fwd, q_back), (0, 30_000));
    }

    #[test]
    fn rate_has_a_baseline_and_counts_deliveries() {
        let mut feed = Feed::new(0);
        // The first PONG only sets the baseline, whatever rx_bytes it carries.
        feed.pong(ms(0), ms(100), ms(50), 5_000_000);
        assert_eq!(feed.dc.stats(feed.start, 0).rate, 0.0);
        for i in 1..=200 {
            feed.pong(ms(10 * i), ms(100), ms(50), 10_000);
        }
        // 10 kB per 10 ms: 1 MB/s.
        let rate = feed.dc.stats(feed.start + ms(2000), 0).rate;
        assert!((rate - 1e6).abs() < 1e5, "{rate}");
    }

    #[test]
    fn forward_in_flight_subtracts_the_delayed_reports_and_nothing_else() {
        let mut feed = Feed::new(0);
        // 1 MB/s, and reports that wait 40 ms on their way back.
        feed.pong(ms(0), ms(100), ms(50), 0);
        for i in 1..=200 {
            let late = if i > 170 { 40 } else { 0 };
            feed.pong(ms(10 * i), ms(100 + late), ms(50), 10_000);
        }
        let end = feed.start + ms(2000);
        // 300 kB were sent, 200 kB are reported delivered.
        let tx = feed.rx + 100_000;
        let in_transit = 100_000;
        // The reports are 40 ms late: 40 kB of the 100 kB were delivered.
        let fwd = feed.dc.fwd_in_flight(end, tx);
        assert!((55_000..65_000).contains(&fwd), "{fwd} of {in_transit}");
        // Without a PONG for a while nothing more is subtracted: the estimate of the rate only decays,
        // so the bytes in flight never shrink, and never exceed what is in transit.
        for wait in [50, 100, 300, 1000] {
            let later = feed.dc.fwd_in_flight(end + ms(wait), tx);
            assert!(later >= fwd && later <= in_transit, "{wait}: {later}");
        }
    }

    #[test]
    fn the_pacer_releases_two_windows_per_round_trip_up_to_a_burst() {
        let mut feed = Feed::new(0);
        feed.dc.set_window(1_000_000);
        // SRTT is 100 ms: 2 * 1 MB / 0.1 s = 20 MB/s, and bursts of 2 frames plus 2 ms of that.
        let now = feed.start;
        feed.dc.srtt = SRTT;
        let cap = 2 * u64::from(FRAME) + 40_000;
        // At first nothing was sent: a full burst.
        assert_eq!(feed.dc.pace_room(now), cap);
        feed.dc.on_sent(now, cap);
        assert_eq!(feed.dc.pace_room(now), 0);
        // 1 ms releases 20 kB, and more time never more than the burst.
        assert_eq!(feed.dc.pace_room(now + ms(1)), 20_000);
        assert_eq!(feed.dc.pace_room(now + ms(1000)), cap);
        // A frame that does not fit fits at the deadline.
        feed.dc.note_paced(now + ms(1), 30_000);
        let deadline = feed.dc.pacer.take_wake().unwrap();
        assert!(deadline > now + ms(1) && feed.dc.pace_room(deadline) >= 30_000);
        assert!(feed.dc.pacer.take_wake().is_none());
    }

    #[test]
    fn the_burst_of_the_pacer_follows_the_lateness_of_the_timer() {
        let mut feed = Feed::new(0);
        feed.dc.set_window(1_000_000);
        feed.dc.srtt = SRTT;
        let now = feed.start;
        let frames = 2 * u64::from(FRAME);
        // 20 MB/s. A timer that is a millisecond late changes nothing: the burst is 2 ms.
        for _ in 0..50 {
            feed.dc.pacer.note_timer(now, now + ms(1));
        }
        assert_eq!(feed.dc.pace_room(now + ms(1000)), frames + 40_000);
        // On Windows the timer is late by 16 ms, and so is the sender: the burst follows,
        // or the rate would be capped at about (2 frames + 40 kB) / 16 ms = 4.5 MB/s.
        for _ in 0..50 {
            feed.dc.pacer.note_timer(now, now + ms(16));
        }
        let room = feed.dc.pace_room(now + ms(1000));
        let expected = frames + 20_000 * 16;
        assert!(room.abs_diff(expected) < 3_000, "{room} {expected}");
    }

    #[test]
    fn counters_that_wrap_are_deltas() {
        let mut feed = Feed::new(0);
        feed.rx = u32::MAX - 5_000;
        feed.pong(ms(0), ms(100), ms(50), 0);
        for i in 1..=100 {
            feed.pong(ms(10 * i), ms(100), ms(50), 10_000);
        }
        assert!(feed.rx < 1_000_000, "the counter wrapped");
        let rate = feed.dc.stats(feed.start + ms(1000), 0).rate;
        assert!((rate - 1e6).abs() < 1e5, "{rate}");
        // In flight is counted modulo 2^32 as well.
        assert_eq!(
            feed.dc
                .fwd_in_flight(feed.start + ms(1000), feed.rx.wrapping_add(7)),
            7
        );
    }

    /// A round trip at `round`: a second of PONGs on an empty queue (1 MB/s), then a PONG
    /// that finds `queue` more of delay on the forward path, after the window is set to `window`.
    /// The window limited sending in the round trip if `limited`.
    fn update(feed: &mut Feed, round: u64, queue: Duration, limited: bool, window: Option<u64>) {
        let start = 2000 + 1200 * round;
        for k in 1..=10 {
            // The window is what limits sending in these, so they are not idle rounds.
            feed.limit(ms(start + 100 * k - 1000));
            feed.pong(ms(start + 100 * k - 1000), ms(100), ms(50), 100_000);
        }
        if let Some(window) = window {
            feed.dc.set_window(window);
        }
        if limited {
            feed.limit(ms(start + 200));
        }
        // Its PING was sent 100 ms before, after the last update.
        feed.pong_ago(ms(start + 200), ms(100), ms(50) + queue, 100_000);
    }

    fn warm(feed: &mut Feed) {
        // A few PONGs for the rate and the base delay.
        for i in 0..10 {
            feed.pong(ms(100 * i), ms(100), ms(50), 100_000);
        }
    }

    /// `g` in units of `alpha`.
    fn units(feed: &Feed) -> u64 {
        feed.dc.growth() / u64::from(ALPHA)
    }

    #[test]
    fn growth_doubles_under_the_gate_and_resets_when_the_queue_reaches_alpha() {
        let mut feed = past_startup(100_000_000);
        // The window limits, the queue is empty and the rate rises: g is alpha, 2, 4, ...
        let seen = rising(&mut feed, 0, 5, ms(0), true);
        assert!(seen.windows(2).all(|w| w[1] <= 2 * w[0]), "{seen:?}");
        assert!(*seen.last().unwrap() >= 4, "{seen:?}");
        // The window did not limit for a round trip (round 5 is skipped, and with it the last
        // block): no growth, g is alpha at the next update.
        round(&mut feed, 6, per_round(6), ms(0), false);
        assert_eq!(units(&feed), 1);
        // And a queue of alpha at once resets it, on the PONG, not at the next update.
        let seen = rising(&mut feed, 7, 11, ms(0), true);
        assert!(*seen.last().unwrap() >= 2, "{seen:?}");
        feed.pong_ago(ms(3700), ms(500), ms(160), 10_000);
        assert_eq!(units(&feed), 1);
    }

    #[test]
    fn a_doubling_window_grows_by_g_per_round_trip_spread_over_the_pongs() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        feed.dc.set_growth(4 * u64::from(ALPHA));
        feed.dc.set_window(1_000_000);
        let before = feed.dc.window();
        // PINGs sent before the update, so g stays: a tenth of the window delivered, a tenth of g.
        for k in 1..=10 {
            feed.limit(ms(1000 + 10 * k));
            feed.pong_ago(ms(1000 + 10 * k), ms(300), ms(50), 100_000);
        }
        let up = (feed.dc.window() - before) as f64;
        let g = 4.0 * f64::from(ALPHA);
        assert!((0.9 * g..1.1 * g).contains(&up), "{up} of {g}");
    }

    #[test]
    fn geometric_growth_stops_at_what_is_on_the_path_when_the_newest_samples_hold_alpha() {
        // 1 MB/s; PONGs 2 ms apart, so the 8 newest samples span 16 ms, much less than the
        // standing minimum of srtt / 2 = 50 ms: they see the queue first.
        let run = |g: u64| {
            let mut feed = Feed::new(0);
            warm(&mut feed);
            feed.dc.set_window(1_000_000);
            feed.dc.set_growth(g);
            feed.in_transit = 600_000;
            // PINGs sent before the update, so no update.
            for i in 0..20 {
                feed.pong_ago(ms(905 + 2 * i), ms(400), ms(50), 2_000);
            }
            // Seven samples with 100 ms of queue (100 kB at 1 MB/s, more than alpha) are not enough:
            // the smallest of the newest eight still has none.
            for i in 0..7 {
                feed.pong_ago(ms(945 + 2 * i), ms(400), ms(150), 2_000);
            }
            assert_eq!(feed.dc.growth(), g);
            assert!(feed.dc.own() < f64::from(ALPHA));
            feed.pong_ago(ms(959), ms(400), ms(150), 2_000);
            feed
        };
        let feed = run(4 * u64::from(ALPHA));
        assert_eq!(feed.dc.growth(), u64::from(ALPHA));
        // The window stops at what is on the path.
        let tx = feed.rx.wrapping_add(feed.in_transit);
        let on_path = feed.dc.fwd_in_flight(feed.start + ms(959), tx);
        assert_eq!(feed.dc.window(), on_path);
        assert!(on_path < 1_000_000);
        // Not geometric (g is alpha): nothing to clamp, the 8 samples do nothing alone.
        let feed = run(u64::from(ALPHA));
        assert!(feed.dc.window() >= 1_000_000);
    }

    #[test]
    fn the_rate_must_rise_only_once_it_has_two_round_trips_of_history() {
        let mut feed = Feed::new(0);
        // The window limits all the time, the queue is empty, the rate is flat: 200 kB/s, a rise of less than half a step (330 kB/s) per round trip while it warms up.
        let mut seen = Vec::new();
        for i in 0..80_u64 {
            feed.limit(ms(10 * i));
            feed.pong(ms(10 * i), ms(100), ms(50), 2_000);
            seen.push((10 * i, feed.dc.growth()));
        }
        // In the first two round trips the rise says nothing: g doubles.
        assert!(
            seen.iter()
                .any(|(at, g)| *at <= 200 && *g > u64::from(ALPHA)),
            "{seen:?}"
        );
        // After that a flat rate keeps g at alpha.
        assert!(
            seen.iter()
                .filter(|(at, _)| *at >= 400)
                .all(|(_, g)| *g == u64::from(ALPHA)),
            "{seen:?}"
        );
    }

    #[test]
    fn limited_means_the_window_blocked_a_frame_within_the_last_round_trip() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        feed.dc.set_window(1_000_000);
        feed.dc.set_growth(u64::from(ALPHA));
        let step = |feed: &mut Feed, at: u64| {
            let before = feed.dc.window();
            feed.pong_ago(ms(at), ms(300), ms(50), 10_000);
            feed.dc.window() - before
        };
        feed.limit(ms(1000));
        // The steps: up inside the round trip (srtt is 100 ms), not after it.
        assert!(step(&mut feed, 1090) > 0);
        assert!(step(&mut feed, 1101) == 0);
        // The growth asks the same question, `limited`, at the update (and nothing else).
        let at = feed.start + ms(1000);
        assert!(feed.dc.limited(at + SRTT));
        assert!(!feed.dc.limited(at + SRTT + ms(1)));
        assert!(!Feed::new(0).dc.limited(at));
    }

    #[test]
    fn the_excess_is_shed_over_the_pongs_of_a_round_trip() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        // 1 MB/s with 400 ms queued: own = 400 kB, far above alpha. The update ends startup.
        update(&mut feed, 0, ms(400), true, Some(1_000_000));
        assert_eq!(units(&feed), 1);
        let own = feed.dc.own();
        let excess = own - f64::from(ALPHA);
        assert!(excess > f64::from(ALPHA), "{own}");
        // Each PONG takes its share of the excess, delivered / W: here a tenth of the window each.
        // PINGs sent before the update, so no update.
        let before = feed.dc.window() as f64;
        feed.pong_ago(ms(2210), ms(500), ms(450), 100_000);
        let cut = before - feed.dc.window() as f64;
        assert!(
            (0.05 * excess..0.25 * excess).contains(&cut),
            "{cut} of {excess}"
        );
        // A window's worth of PONGs takes about all of it (the shares grow as the window shrinks).
        for k in 1..=9 {
            feed.pong_ago(ms(2210 + 10 * k), ms(500), ms(450), 100_000);
        }
        let cut = before - feed.dc.window() as f64;
        assert!(cut >= 0.9 * excess, "{cut} of {excess}");
    }

    #[test]
    fn after_startup_the_window_steps_by_alpha_per_round_trip_spread_over_the_pongs() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        // Between alpha and 2 alpha queued (1 MB/s: 65-130 ms): startup ends.
        update(&mut feed, 0, ms(100), true, Some(1_000_000));
        let after_update = feed.dc.window();
        // PONGs (PINGs sent before the update, so no update) that deliver a tenth of the window
        // (at the same 1 MB/s): down by a tenth of alpha in all.
        for k in 1..=10 {
            feed.pong_ago(ms(2200 + 10 * k), ms(300), ms(150), 10_000);
        }
        let down = after_update - feed.dc.window();
        assert!((6_000..=7_000).contains(&down), "{down}");
        // Below alpha, and the window limits: up by a tenth of alpha.
        update(&mut feed, 1, ms(10), true, Some(1_000_000));
        let after_update = feed.dc.window();
        for k in 1..=10 {
            feed.pong_ago(ms(3400 + 10 * k), ms(300), ms(60), 10_000);
        }
        let up = feed.dc.window() - after_update;
        assert!((6_000..=7_000).contains(&up), "{up}");
    }

    #[test]
    fn an_unused_window_neither_grows_nor_decays() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        update(&mut feed, 0, ms(100), true, Some(1_000_000));
        let w = feed.dc.window();
        // Round trips in which the window never limited sending, below the target: it neither grows
        // nor shrinks, however much (or little) the application sends.
        for i in 0..10 {
            feed.pong(ms(3000 + 100 * i), ms(100), ms(50), 100_000);
        }
        assert_eq!(feed.dc.window(), w);
        for i in 10..40 {
            feed.pong(ms(3000 + 100 * i), ms(100), ms(50), 100_000);
        }
        assert_eq!(feed.dc.window(), w);
    }

    #[test]
    fn a_clump_of_reports_moves_the_window_by_one_step_at_most() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        // Past startup.
        update(&mut feed, 0, ms(100), true, Some(1_000_000));
        let alpha = u64::from(ALPHA);
        // The window limits, the queue is below alpha, and one PONG reports 10 MB at once, 150
        // windows: the window grows by alpha, not by 150 alpha.
        feed.dc.set_window(65_536);
        feed.limit(ms(2000 + 1200 * 3));
        feed.pong_ago(ms(2000 + 1200 * 3), ms(500), ms(50), 10_000_000);
        assert_eq!(feed.dc.window(), 65_536 + alpha);
    }

    #[test]
    fn the_window_never_drops_below_four_frames() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        // The steps, the excess cut and the stop of the growth all stop at the same floor. This
        // update stops the growth; the next PONG sheds the 400 ms of queue.
        update(&mut feed, 0, ms(400), true, Some(u64::from(FRAME) + 10));
        feed.pong_ago(ms(2210), ms(500), ms(450), 100_000);
        assert_eq!(feed.dc.window(), 4 * u64::from(FRAME));
        update(
            &mut feed,
            1,
            ms(100),
            true,
            Some(4 * u64::from(FRAME) + 100),
        );
        assert_eq!(feed.dc.window(), 4 * u64::from(FRAME));
    }

    #[test]
    fn one_update_per_round_trip() {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        feed.dc.set_window(1_000_000);
        // Two PONGs in a row for PINGs sent before the first update count once.
        let at = ms(1000);
        feed.pong(at, ms(100), ms(50), 100_000);
        let w = feed.dc.window();
        // The PING of this one was sent at 1000 + 5 - 90 ms, before the update at 1000 + 5.
        feed.pong(at + ms(10), ms(100), ms(50), 100_000);
        assert_eq!(feed.dc.window(), w);
    }

    fn past_startup(window: u64) -> Feed {
        let mut feed = Feed::new(0);
        warm(&mut feed);
        update(&mut feed, 0, ms(100), true, Some(window));
        feed
    }

    /// One round trip of 100 ms (an update at its end) after the round `k - 1`: ten PONGs that
    /// each deliver `per_pong` bytes and find `queue` of delay on the forward path.
    fn round(feed: &mut Feed, k: u64, per_pong: u32, queue: Duration, limited: bool) {
        round_with(feed, k, per_pong, queue, limited, None);
    }

    /// Like [`round`], with the window set before the last PONG, the one that updates.
    fn round_with(
        feed: &mut Feed,
        k: u64,
        per_pong: u32,
        queue: Duration,
        limited: bool,
        window: Option<u64>,
    ) {
        let t0 = 2300 + 100 * k;
        for i in 1..=10 {
            if limited {
                feed.limit(ms(t0 + 10 * i));
            }
            // The PINGs of the first nine were sent before the last update, the last one after.
            let ago = if i == 10 { 95 } else { 300 };
            if let (10, Some(window)) = (i, window) {
                feed.dc.set_window(window);
            }
            feed.pong_ago(ms(t0 + 10 * i), ms(ago), ms(50) + queue, per_pong);
        }
    }

    /// The bytes per PONG of the round `k` of a rate that rises by about 3 MB/s per round.
    fn per_round(k: u64) -> u32 {
        10_000 + 30_000 * u32::try_from(k).unwrap()
    }

    /// The rounds `from..to` with a rate that rises by about 3 MB/s per round.
    fn rising(feed: &mut Feed, from: u64, to: u64, queue: Duration, limited: bool) -> Vec<u64> {
        (from..to)
            .map(|k| {
                round(feed, k, per_round(k), queue, limited);
                units(feed)
            })
            .collect()
    }

    #[test]
    fn growth_doubles_while_it_buys_rate_on_an_empty_queue_and_the_cap_holds() {
        let mut feed = past_startup(10_000_000);
        let seen = rising(&mut feed, 0, 6, ms(0), true);
        assert!(seen.iter().all(|v| v.is_power_of_two()), "{seen:?}");
        assert!(seen.windows(2).all(|w| w[1] <= 2 * w[0]), "{seen:?}");
        assert!(seen.contains(&4), "{seen:?}");
        // The step of a PONG is v alpha times its share.
        let v = units(&feed);
        let before = feed.dc.window();
        feed.limit(ms(3000));
        feed.pong_ago(ms(3000), ms(300), ms(50), 10_000);
        let step = (feed.dc.window() - before) as f64;
        let want = v as f64 * f64::from(ALPHA) * 10_000.0 / before as f64;
        assert!((step - want).abs() <= want * 0.01 + 2.0, "{step} {want}");
        // The cap, g <= W: with a window of 5 alpha, g stops there however long the rate rises.
        let mut feed = past_startup(10_000_000);
        let mut top = 0;
        for k in 0..8 {
            let window = Some(5 * u64::from(ALPHA));
            round_with(&mut feed, k, per_round(k), ms(0), true, window);
            assert!(feed.dc.growth() <= feed.dc.window(), "{}", feed.dc.window());
            top = top.max(units(&feed));
        }
        assert!(top >= 4, "{top}");
    }

    #[test]
    fn doubling_stops_when_one_pong_shows_half_alpha_queued() {
        let mut feed = past_startup(10_000_000);
        let seen = rising(&mut feed, 0, 4, ms(0), true);
        assert!(seen.iter().any(|v| *v > 1), "{seen:?}");
        // A queue of 0.75 alpha at the present rate in a whole round: above alpha / 2, below alpha.
        let per_pong = per_round(3);
        let rate = feed.dc.stats(feed.start + ms(2700), feed.rx).rate;
        let queue = Duration::from_secs_f64(0.75 * f64::from(ALPHA) / rate);
        round(&mut feed, 4, per_pong, queue, true);
        assert!(feed.dc.own() >= f64::from(ALPHA) / 2.0, "{}", feed.dc.own());
        assert!(feed.dc.own() < f64::from(ALPHA), "{}", feed.dc.own());
        assert_eq!(units(&feed), 1);
    }

    #[test]
    fn doubling_stops_when_the_window_did_not_limit() {
        let mut feed = past_startup(10_000_000);
        let seen = rising(&mut feed, 0, 4, ms(0), true);
        assert!(*seen.last().unwrap() > 1, "{seen:?}");
        // A round in which the rate keeps rising, but the window did not limit sending for a round
        // trip (round 4 is skipped, and with it the last block).
        feed.dc.set_window(10_000_000);
        round(&mut feed, 5, 160_000, ms(0), false);
        assert_eq!(units(&feed), 1);
    }

    #[test]
    fn doubling_stops_when_the_rate_is_flat() {
        let mut feed = past_startup(10_000_000);
        // The window limits and the queue is empty, but the rate does not rise: the link or the
        // TCP below it is the limit, not the window.
        let seen: Vec<_> = (0..10)
            .map(|k| {
                round(&mut feed, k, 50_000, ms(0), true);
                units(&feed)
            })
            .collect();
        assert!(seen.iter().skip(4).all(|v| *v == 1), "{seen:?}");
    }

    #[test]
    fn growth_ends_at_once_when_the_queue_reaches_alpha_and_the_decrease_is_not_multiplied() {
        let mut feed = past_startup(10_000_000);
        let seen = rising(&mut feed, 0, 4, ms(0), true);
        assert!(*seen.last().unwrap() >= 2, "{seen:?}");
        // PINGs sent before the update: no update, so only the rule of the PONG can reset v.
        let mut at = 2700;
        let mut cut = None;
        for _ in 0..30 {
            let before = feed.dc.window();
            feed.pong_ago(ms(at), ms(500), ms(160), 10_000);
            at += 10;
            if feed.dc.window() < before {
                cut = Some((before, before - feed.dc.window()));
                break;
            }
        }
        let (before, cut) = cut.expect("a decrease");
        assert_eq!(units(&feed), 1);
        let step = f64::from(ALPHA).max(feed.dc.own() - f64::from(ALPHA));
        let want = step * 10_000.0 / before as f64;
        assert!((cut as f64 - want).abs() <= 2.0, "{cut} {want}");
    }
}
