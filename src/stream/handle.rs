//! The connection's handle to a stream, which takes the inbound frames.

use std::num::NonZeroU32;

use tokio::time::Instant;

use crate::{
    buffer::Data,
    error::{StreamError, StreamViolation},
    stream::{ConnFlow, FinState, RxFlow, StreamRef, outbound::Ctx},
    wire::header::ControlFlags,
};

/// The connection's handle to a stream.
pub struct StreamHandle(pub(super) StreamRef);

impl StreamHandle {
    /// Updates the stream with the stream part of a received frame.
    ///
    /// The parts of the frame are processed in the order of the protocol:
    /// `WINDOW`, `DATA`, `FIN_WRITE` (with `ABORT`), `FIN_READ`. `SYN` is the business of the caller.
    ///
    /// Returns the new [`FinState`] of the whole stream.
    pub fn received(
        &self,
        credit: Option<NonZeroU32>,
        data: Data,
        flags: ControlFlags,
        tx: &mut ConnFlow,
        rx: &mut RxFlow,
    ) -> Result<FinState, StreamViolation> {
        let mut state = self.0.lock();
        let mut ctx = Ctx {
            flow: tx,
            me: &self.0,
            now: Instant::now(),
        };
        if let Some(credit) = credit {
            state.outbound.received_window_update(credit, &mut ctx)?;
        }
        state.inbound.received_data(data, &mut rx.pool)?;
        if flags.fin_write {
            state
                .inbound
                .received_fin_write(flags.abort, &self.0.credit_task)?;
        }
        if flags.fin_read {
            state.outbound.received_fin_read(&mut ctx)?;
        }
        Ok(state.fin_state())
    }

    /// Closes the stream, because the connection is gone.
    ///
    /// The application reads the data that arrived, and then the `reason`. Writing fails with the `reason`.
    /// A direction that ended before keeps the end it got.
    /// Does nothing if another user of the stream panicked while holding its lock.
    pub fn abort(self, reason: StreamError) {
        let Some(mut state) = self.0.try_lock() else {
            return;
        };
        state.inbound.abort(reason, &self.0.credit_task);
        state.outbound.abort(reason, &self.0.data_task);
    }
}
