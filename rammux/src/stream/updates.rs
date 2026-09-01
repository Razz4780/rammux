//! A stream's side of the connection's task selector: what it wants to
//! put on the wire next.

use std::{
    ops::ControlFlow,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_selector::{
    selector::{BorrowedMut, Removed},
    task::Task,
};
use bytes::Bytes;

use crate::{
    StreamId,
    codec::encoder::EncoderItem,
    global_pool::GlobalPool,
    header::ControlFlags,
    stream::{FinState, SharedStreamState},
};

/// One stream's entry in the connection's selector: polled for whatever
/// that stream wants to put on the wire next.
pub(crate) struct StreamUpdates {
    /// Stream being polled.
    pub(super) id: StreamId,
    /// Whether the next frame still has to carry `SYN`.
    pub(super) syn: bool,
    /// State shared with the application-facing handle.
    pub(super) state: Arc<Mutex<SharedStreamState>>,
}

impl Task<GlobalPool> for StreamUpdates {
    type Cont = (StreamUpdate, FinState);
    type Break = (StreamUpdate, FinState);
    type Output = (StreamUpdate, FinState);

    fn poll_progress(
        self: Pin<&mut Self>,
        global: &mut GlobalPool,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        if global.probe_paused() {
            // A link-clearing probe is in progress: no stream frames may
            // travel until it completes.
            let mut guard = this.state.lock().unwrap();
            guard.updates_poller.register(cx.waker());
            return Poll::Pending;
        }
        let mut update = StreamUpdate {
            id: this.id,
            window_update: 0,
            data: Default::default(),
            flags: ControlFlags::default(),
        };
        let mut is_pending = true;

        let mut guard = this.state.lock().unwrap();
        if let Poll::Ready((window_update, fin_read)) = guard.inbound.poll_update(global) {
            update.window_update = window_update;
            update.flags.fin_read = fin_read;
            is_pending = false;
        }
        let mut transit_blocked = false;
        let transit_credit = global
            .transit_send
            .as_mut()
            .map(|transit| &mut transit.credit);
        if let Poll::Ready((data, fin_write)) = guard
            .outbound
            .poll_update(transit_credit, &mut transit_blocked)
        {
            update.data = data;
            update.flags.fin_write = fin_write;
            is_pending = false;
        }
        if is_pending {
            global.transit_blocked |= transit_blocked;
            guard.updates_poller.register(cx.waker());
            return Poll::Pending;
        }
        let inbound_fin = guard.inbound.fin_state();
        let outbound_fin = guard.outbound.fin_state();
        drop(guard);

        // At this point, `update` cannot be empty.
        update.flags.syn = std::mem::take(&mut this.syn);

        let fin_state = inbound_fin.and(outbound_fin);
        let update = if fin_state.sent {
            // If we sent both fins, this stream of updates is done.
            ControlFlow::Break((update, fin_state))
        } else {
            // Otherwise, this stream of updates should be polled again.
            ControlFlow::Continue((update, fin_state))
        };
        Poll::Ready(update)
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut GlobalPool,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(value)
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut GlobalPool,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some(value)
    }
}

/// What a stream wants to send: window credit, payload, or both, plus
/// whatever lifecycle bits are due. Converts into one or two frames.
pub struct StreamUpdate {
    /// Stream this belongs to.
    pub id: StreamId,
    /// Receive window credit to return, `0` for none.
    pub window_update: u32,
    /// Payload to send, empty for none.
    pub data: Bytes,
    /// Lifecycle bits to carry.
    pub flags: ControlFlags,
}

impl From<StreamUpdate> for EncoderItem {
    fn from(value: StreamUpdate) -> Self {
        if value.data.is_empty() {
            // Single WINDOW_UPDATE frame.
            EncoderItem::new_window_update(value.id, value.flags, value.window_update)
        } else if value.window_update > 0 {
            // Two frames, WINDOW_UPDATE and DATA.
            EncoderItem::new_window_update_and_data(
                value.id,
                value.flags,
                value.window_update,
                value.data,
            )
        } else {
            // Single DATA frame.
            EncoderItem::new_data(value.id, value.flags, value.data)
        }
    }
}
