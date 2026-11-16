//! The part of the flow control that the streams of a connection share.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Waker},
    time::Duration,
};

use tokio::time::Sleep;

use crate::{
    config::RammuxConfig,
    flow::{Advance, Pool, Srtt},
    stream::{StreamRef, turns::Turns},
};

/// The state that our outbound streams share: what the sending direction of every stream draws on.
///
/// It is the strategy of the selector that polls the [`DataUpdates`](super::updates::DataUpdates),
/// so a stream gets it when it is polled, and the connection gets it when it hands an inbound frame
/// to a stream.
pub struct ConnFlow {
    /// Smoothed round trip time of the connection.
    pub srtt: Srtt,
    /// The peer's receive pool, which our outbound streams use.
    pub tx_pool: Pool,
    /// Streams that wait for their turn to borrow from [`Self::tx_pool`].
    pub turns: Turns,
    /// The unproven bytes that our outbound streams drew from [`Self::tx_pool`]. Zero until [`Self::with_advance`].
    pub advance: Advance,
}

impl ConnFlow {
    /// Creates the state of a new connection, with the size of the peer's receive pool.
    pub fn new(tx_pool: u32) -> Self {
        Self {
            srtt: Srtt::default(),
            tx_pool: Pool::new(tx_pool),
            turns: Turns::default(),
            advance: Advance::default(),
        }
    }

    /// Lets the streams draw the advance that the config asks for.
    pub fn with_advance(mut self, config: &RammuxConfig) -> Self {
        self.advance = Advance::new(
            config.remote_recv_pool,
            config.advance_grant,
            config.advance_budget,
        );
        self
    }

    /// Takes the stream out of the turn queue, and lets the next one borrow if there is room.
    pub fn pass_turn(&mut self, stream: &StreamRef) {
        if self.turns.leave(stream) && self.tx_pool.has_room() {
            self.turns.wake_front();
        }
    }
}

/// The shortest period of [`Settle`], however small the round trip time is.
pub const SETTLE_PERIOD_MIN: Duration = Duration::from_millis(1);

/// The state that the streams share as receivers: what the receiving direction of every stream draws on.
///
/// It is the strategy of the selector that polls the [`CreditUpdates`](super::updates::CreditUpdates).
pub struct RxFlow {
    /// Smoothed round trip time of the connection.
    pub srtt: Srtt,
    /// Our receive pool, which the inbound streams use.
    pub pool: Pool,
    /// Wakes the streams that hold credit back, so that they look at it again.
    pub settle: Settle,
}

impl RxFlow {
    /// Creates the state of a new connection, with the size of our receive pool.
    pub fn new(pool: u32) -> Self {
        Self {
            srtt: Srtt::default(),
            pool: Pool::new(pool),
            settle: Settle::default(),
        }
    }

    /// Returns how often [`Self::settle`] wakes the streams that hold credit back: `max(srtt / 8, 1 ms)`.
    /// [`None`] without a round trip sample.
    ///
    /// The floor keeps a tiny round trip time from spinning the timer.
    pub fn settle_period(&self) -> Option<Duration> {
        self.srtt
            .get()
            .map(|srtt| (srtt / 8).max(SETTLE_PERIOD_MIN))
    }

    /// Returns how long after its last credit a stream counts as no longer keeping its pace: `srtt / 4`.
    /// [`None`] without a round trip sample.
    ///
    /// A reader that keeps its pace returns credit about `CREDIT_STEP_DIVISOR` times per loop, so a quarter of
    /// a loop without credit means that it slowed down or stopped. Together with [`Self::settle_period`], the
    /// credit it held back goes out within about three eighths of a loop.
    pub fn settle_after(&self) -> Option<Duration> {
        self.srtt.get().map(|srtt| srtt / 4)
    }
}

/// One timer that the streams of a connection share, for the credit they hold back.
///
/// A stream that returns credit in steps larger than `T_c` registers here, and is woken when the timer elapses.
/// It then decides for itself whether it waited long enough; one that did not registers again.
/// A wake-up may be spurious, so a stream that is woken never relies on having waited.
#[derive(Default)]
pub struct Settle {
    /// Created on first use: a connection whose streams never hold credit back never touches the timer.
    sleep: Option<Pin<Box<Sleep>>>,
    /// Whether [`Self::sleep`] runs for the wakers in [`Self::wakers`].
    armed: bool,
    /// The streams to wake when the timer elapses, each once.
    ///
    /// A stream may register many times within one period, so a waker that wakes the same task as one in the
    /// list is not added. A stream that closed stays here until the timer elapses; waking it is harmless.
    wakers: Vec<Waker>,
}

impl Settle {
    /// Wakes `waker` when the timer elapses, and arms the timer `period` from now if it is not armed.
    ///
    /// The timer elapses only if it is polled, see [`Self::poll`].
    pub fn register(&mut self, waker: &Waker, period: Duration) {
        if !self.armed {
            let deadline = crate::deadline(period);
            match &mut self.sleep {
                Some(sleep) => sleep.as_mut().reset(deadline),
                None => self.sleep = Some(Box::pin(tokio::time::sleep_until(deadline))),
            }
            self.armed = true;
        }
        if !self.wakers.iter().any(|known| known.will_wake(waker)) {
            self.wakers.push(waker.clone());
        }
    }

    /// Polls the timer. When it elapsed, disarms it and wakes every registered stream.
    ///
    /// Must be polled again after a stream registered, so that the timer can wake the caller.
    pub fn poll(&mut self, cx: &mut Context<'_>) {
        if !self.armed {
            return;
        }
        let Some(sleep) = &mut self.sleep else {
            return;
        };
        if sleep.as_mut().poll(cx).is_pending() {
            return;
        }
        self.armed = false;
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }

    /// Returns whether no stream ever registered.
    #[cfg(test)]
    pub fn is_unused(&self) -> bool {
        self.sleep.is_none()
    }
}
