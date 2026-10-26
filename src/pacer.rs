//! The pacer of `TransitControl::CopaLike`: a token bucket, and the timer that wakes the sender
//! when a frame it kept back fits.
//!
//! The rate and the size of a frame come from the connection window, see `DelayControl`; everything
//! else about pacing lives here.

use std::{future::Future, pin::Pin, task::Context, time::Duration};

use tokio::time::{Instant, Sleep};

use crate::transit::to_bytes;

/// The pacer lets out at least what it releases in this time at once, see [`Pacer::cap`].
const PACING_BURST: Duration = Duration::from_millis(2);

/// Weight of a new sample in the smoothed lateness of the timer of the pacer.
const LATENESS_GAIN: f64 = 1.0 / 8.0;

/// The pacer: bytes that may be sent now, and the timer for the frame it kept back.
///
/// Bytes accrue at the pacing rate, which the caller passes in as `rate` (bytes per second), up to
/// [`Self::cap`], which adds `2 * frame_limit` to what the rate releases in a burst.
pub struct Pacer {
    /// Bytes that may be sent, as of `at`. See [`Self::room`].
    tokens: f64,
    at: Instant,
    /// When the pacer will have room for the frame it kept back, if it kept one back, and the timer
    /// has not been armed for it yet.
    wake: Option<Instant>,
    /// Wakes the sender when the pacer has room again. Created when first armed.
    timer: Option<Pin<Box<Sleep>>>,
    /// When the armed timer is to wake up, to measure how late it does. [`None`] if it is not armed.
    armed_for: Option<Instant>,
    /// How late the timer of the pacer wakes up, smoothed: about a millisecond on Linux, up to
    /// 15.6 ms on Windows. A sender that is clocked by the timer sends one burst per wake-up.
    lateness: Duration,
}

impl Pacer {
    /// Creates a pacer with a full burst of room.
    pub fn new(now: Instant) -> Self {
        Self {
            tokens: f64::INFINITY,
            at: now,
            wake: None,
            timer: None,
            armed_for: None,
            lateness: Duration::ZERO,
        }
    }

    /// The most the pacer lets out at once: two frames, and what it releases in the time the timer
    /// needs to wake the sender, `PACING_BURST` at least. A sender that is woken every `t` can send
    /// at most this much per wake-up, so with less the timer would cap the rate: with a burst
    /// of 2 ms, a timer that wakes every 15.6 ms (Windows) would cap 150 Mbit/s at about 55.
    fn cap(&self, rate: f64, frame_limit: u64) -> f64 {
        let burst = PACING_BURST.max(self.lateness);
        (2 * frame_limit) as f64 + rate * burst.as_secs_f64()
    }

    /// The room the pacer leaves: bytes released at `rate` since the last send, up to
    /// [`Self::cap`].
    ///
    /// Without it, data leaves when delivery reports arrive. With bulk both ways the reports wait
    /// behind the peer's data and arrive in clumps (ACK compression), so the window opens in
    /// clumps and is sent in bursts at the speed of the local link. The bottleneck queues the burst,
    /// then idles until the next clump: the link is idle half the time while it holds a queue.
    pub fn room(&self, now: Instant, rate: f64, frame_limit: u64) -> u64 {
        to_bytes(self.tokens_at(now, rate, frame_limit))
    }

    fn tokens_at(&self, now: Instant, rate: f64, frame_limit: u64) -> f64 {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        (self.tokens + rate * elapsed).min(self.cap(rate, frame_limit))
    }

    /// Takes `n` bytes of DATA that were sent out of the pacer's room.
    pub fn spend(&mut self, now: Instant, n: u64, rate: f64, frame_limit: u64) {
        let tokens = self.tokens_at(now, rate, frame_limit);
        self.tokens = (tokens - n as f64).max(0.0);
        self.at = now;
    }

    /// Records that the pacer kept `wanted` bytes from being sent: they fit at the deadline.
    pub fn hold(&mut self, now: Instant, wanted: u64, rate: f64, frame_limit: u64) {
        let room = self.room(now, rate, frame_limit);
        let missing = wanted.saturating_sub(room) as f64;
        let wait = Duration::from_secs_f64(missing / rate) + Duration::from_micros(1);
        self.wake = Some(now + wait);
    }

    /// Takes the time when the pacer has room for the frame it kept back.
    pub fn take_wake(&mut self) -> Option<Instant> {
        self.wake.take()
    }

    /// Arms the timer for a frame that was kept back, and polls it: [`true`] if it has fired, and
    /// the sender is to try again. Otherwise `cx` is woken when it fires.
    pub fn poll_timer(&mut self, cx: &mut Context<'_>) -> bool {
        if let Some(deadline) = self.take_wake() {
            match &mut self.timer {
                Some(timer) => timer.as_mut().reset(deadline),
                None => self.timer = Some(Box::pin(tokio::time::sleep_until(deadline))),
            }
            self.armed_for = Some(deadline);
        }
        let (Some(deadline), Some(timer)) = (self.armed_for, &mut self.timer) else {
            return false;
        };
        if timer.as_mut().poll(cx).is_pending() {
            return false;
        }
        self.armed_for = None;
        self.note_timer(deadline, Instant::now());
        true
    }

    /// Takes how late the timer of the pacer woke the sender: it was set for `deadline`.
    ///
    /// Limitation: the sample is the time from `deadline` to the moment the connection task noticed the
    /// timer, which also includes delays that are not the timer's (a transport that was not writable,
    /// a batch of inbound frames, an application that did not poll). One long delay of that kind raises
    /// the smoothed lateness, and with it the pacer's burst, for many wake-ups. The burst only grows,
    /// never shrinks the rate, and the lateness decays by a factor of 7/8 (`1 - LATENESS_GAIN`) per pacer
    /// wake-up: about 16 wake-ups to fall to an eighth.
    pub fn note_timer(&mut self, deadline: Instant, woke_at: Instant) {
        let late = woke_at.saturating_duration_since(deadline).as_secs_f64();
        let smoothed = self.lateness.as_secs_f64();
        self.lateness =
            Duration::from_secs_f64((smoothed + (late - smoothed) * LATENESS_GAIN).max(0.0));
    }
}
