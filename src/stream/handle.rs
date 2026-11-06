//! The connection's handle to a stream, which takes the inbound frames.

use std::num::NonZeroU32;

use tokio::time::Instant;

use crate::{
    buffer::Data,
    error::StreamError,
    stream::{ConnFlow, FinState, StreamRef, outbound::Ctx},
    wire::header::ControlFlags,
};

/// The connection's handle to a stream.
pub struct StreamHandle(pub(super) StreamRef);

impl StreamHandle {
    /// Updates the stream with the stream part of a received frame.
    ///
    /// The parts of the frame are processed in the order of the protocol:
    /// `WINDOW`, `DATA`, `FIN_WRITE`, `FIN_READ`. `SYN` is the business of the caller.
    ///
    /// Returns the new [`FinState`] of the whole stream.
    pub fn received(
        &self,
        credit: Option<NonZeroU32>,
        data: Data,
        flags: ControlFlags,
        flow: &mut ConnFlow,
    ) -> Result<FinState, StreamError> {
        let mut state = self.0.lock();
        let mut ctx = Ctx {
            flow,
            me: &self.0,
            now: Instant::now(),
        };
        if let Some(credit) = credit {
            state.outbound.received_window_update(credit, &mut ctx)?;
        }
        state.inbound.received_data(data, &mut ctx.flow.rx_pool)?;
        if flags.fin_write {
            state.inbound.received_fin_write(&self.0.updates)?;
        }
        if flags.fin_read {
            state.outbound.received_fin_read(&mut ctx)?;
        }
        Ok(state.fin_state())
    }

    /// Closes the stream, because the connection is gone.
    ///
    /// The application reads the data that arrived, and then the end of the stream. Writing fails.
    /// Does nothing if another user of the stream panicked while holding its lock.
    pub fn abort(self) {
        let Some(mut state) = self.0.try_lock() else {
            return;
        };
        let _ = state.inbound.received_fin_write(&self.0.updates);
        state.outbound.abort(&self.0.updates);
    }
}
