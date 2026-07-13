use std::sync::{Arc, Mutex};

use crate::{
    buffer::Data,
    error::StreamError,
    stream::{FinState, SharedStreamState},
};

/// Handle to a virtual rammux stream.
pub struct StreamHandle(pub(super) Arc<Mutex<SharedStreamState>>);

impl StreamHandle {
    /// Updates the state of this stream with a received `DATA` frame.
    ///
    /// Returns the new [`FinState`] of the whole stream.
    pub fn received_data(
        &mut self,
        data: Data,
        fin_read: bool,
        fin_write: bool,
    ) -> Result<FinState, StreamError> {
        let mut guard = self.0.lock().unwrap();
        let SharedStreamState {
            inbound,
            outbound,
            updates_poller,
        } = &mut *guard;
        inbound.received_data(data)?;
        if fin_write {
            inbound.received_fin_write(updates_poller)?;
        }
        if fin_read {
            outbound.received_fin_read(updates_poller)?;
        }
        Ok(inbound.fin_state().and(outbound.fin_state()))
    }

    /// Updates the state of this stream with a received `WINDOW_UPDATE` frame.
    ///
    /// Returns the new [`FinState`] of the whole stream.
    pub fn received_window_update(
        &mut self,
        update: u32,
        fin_read: bool,
        fin_write: bool,
    ) -> Result<FinState, StreamError> {
        let mut guard = self.0.lock().unwrap();
        let SharedStreamState {
            inbound,
            outbound,
            updates_poller,
        } = &mut *guard;
        outbound.received_window_update(update, updates_poller)?;
        if fin_read {
            outbound.received_fin_read(updates_poller)?;
        }
        if fin_write {
            inbound.received_fin_write(updates_poller)?;
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
