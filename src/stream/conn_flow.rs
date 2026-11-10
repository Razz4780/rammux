//! The part of the flow control that the streams of a connection share.

use crate::{
    config::RammuxConfig,
    flow::{Advance, Pool, Srtt},
    stream::{
        StreamRef,
        turns::{BudgetWaiters, Turns},
    },
};

/// The state that the streams of a connection share.
///
/// It is the strategy of the selector that polls the [`StreamUpdates`](super::updates::StreamUpdates),
/// so a stream gets it when it is polled, and the connection gets it when it hands an inbound frame
/// to a stream.
pub struct ConnFlow {
    /// Smoothed round trip time of the connection.
    pub srtt: Srtt,
    /// Our receive pool, which the inbound streams use.
    pub rx_pool: Pool,
    /// The peer's receive pool, which our outbound streams use.
    pub tx_pool: Pool,
    /// Streams that wait for their turn to borrow from [`Self::tx_pool`].
    pub turns: Turns,
    /// The unproven bytes that our outbound streams drew from [`Self::tx_pool`]. None until [`Self::with_advance`].
    pub advance: Advance,
    /// Streams that wait for room in [`Self::advance`].
    pub budget_waiters: BudgetWaiters,
}

impl ConnFlow {
    /// Creates the state of a new connection, with the sizes of the receive pools.
    pub fn new(rx_pool: u32, tx_pool: u32) -> Self {
        Self {
            srtt: Srtt::default(),
            rx_pool: Pool::new(rx_pool),
            tx_pool: Pool::new(tx_pool),
            turns: Turns::default(),
            advance: Advance::default(),
            budget_waiters: BudgetWaiters::default(),
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

impl ConnFlow {
    /// Takes the stream out of the queue for the budget, and lets the next one draw if there is room.
    ///
    /// Same as [`Self::pass_turn`]: a woken stream that does not wait for the budget any more
    /// (it drew, or something else blocks it) must not swallow the wake-up of those behind it.
    pub fn pass_budget(&mut self, stream: &StreamRef) {
        if self.budget_waiters.leave(stream) {
            self.wake_budget_front();
        }
    }

    /// Takes the stream out of both queues, because it waits for neither.
    pub fn leave_queues(&mut self, stream: &StreamRef) {
        self.pass_turn(stream);
        self.pass_budget(stream);
    }

    /// Wakes the stream that waited longest for the budget, if the budget has room.
    ///
    /// Called when a repayment or a `FIN_READ` returned some of it.
    pub fn wake_budget_front(&self) {
        if self.advance.free() > 0 {
            self.budget_waiters.wake_front();
        }
    }
}
