use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
};

use tokio::time::Instant;

use crate::{
    buffer::Data,
    conn_shared::ConnShared,
    error::StreamError,
    stream::{FinState, SharedStreamState, outbound::Ctx},
};

/// Handle to a virtual rammux stream.
pub struct StreamHandle(pub(super) Arc<Mutex<SharedStreamState>>);

impl StreamHandle {
    /// Updates the state of this stream with the stream part of a received frame.
    ///
    /// The parts of the frame are processed in the order of the protocol:
    /// `WINDOW`, `DATA`, `FIN_WRITE`, `FIN_READ`.
    ///
    /// Returns the new [`FinState`] of the whole stream.
    pub fn received(
        &mut self,
        credit: Option<NonZeroU32>,
        data: Data,
        fin_write: bool,
        fin_read: bool,
        global: &mut ConnShared,
    ) -> Result<FinState, StreamError> {
        let mut guard = self.0.lock().unwrap();
        let SharedStreamState {
            inbound,
            outbound,
            updates_poller,
        } = &mut *guard;
        // Credit and FIN_READ release a part of the peer's pool, and the queue for it gets a turn.
        let mut ctx = Ctx {
            global,
            me: &self.0,
            poller: updates_poller,
            now: Instant::now(),
        };
        if let Some(credit) = credit {
            outbound.received_window_update(credit, &mut ctx)?;
        }
        inbound.received_data(data, ctx.global)?;
        if fin_write {
            inbound.received_fin_write(ctx.poller)?;
        }
        if fin_read {
            outbound.received_fin_read(&mut ctx)?;
        }
        Ok(inbound.fin_state().and(outbound.fin_state()))
    }

    /// Attempts to abort this stream.
    ///
    /// This is a noop if the stream is already dead or the internal mutex is poisoned.
    pub fn try_abort(self) {
        let Ok(mut guard) = self.0.lock() else {
            return;
        };
        let SharedStreamState {
            inbound,
            outbound,
            updates_poller,
        } = &mut *guard;
        let _ = inbound.received_fin_write(updates_poller);
        outbound.abort(updates_poller);
    }
}
