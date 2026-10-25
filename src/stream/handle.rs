use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::Data,
    error::StreamError,
    global_pool::GlobalPool,
    stream::{FinState, SharedStreamState},
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
        global: &mut GlobalPool,
    ) -> Result<FinState, StreamError> {
        let mut guard = self.0.lock().unwrap();
        let SharedStreamState {
            inbound,
            outbound,
            updates_poller,
        } = &mut *guard;
        if let Some(credit) = credit {
            outbound.received_window_update(credit.get(), updates_poller)?;
        }
        inbound.received_data(data, global)?;
        if fin_write {
            inbound.received_fin_write(updates_poller)?;
        }
        if fin_read {
            outbound.received_fin_read(updates_poller)?;
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
        let _ = outbound.received_fin_read(updates_poller);
    }
}
