use std::{
    ops::ControlFlow,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_selector::pollable::Pollable;
use bytes::Bytes;

use crate::{
    StreamId,
    codec::encoder::EncoderItem,
    global_pool::GlobalPool,
    header::ControlFlags,
    stream::{FinState, SharedStreamState},
};

pub(crate) struct StreamUpdates {
    pub(super) id: StreamId,
    pub(super) syn: bool,
    pub(super) state: Arc<Mutex<SharedStreamState>>,
}

impl<'a> Pollable<'a, (), GlobalPool> for StreamUpdates {
    type Progress = (StreamUpdate, FinState);

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &'a (),
        global: &mut GlobalPool,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>> {
        let this = self.get_mut();
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
        if let Poll::Ready((data, fin_write)) = guard.outbound.poll_update() {
            update.data = data;
            update.flags.fin_write = fin_write;
            is_pending = false;
        }
        if is_pending {
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
            ControlFlow::Break(Some((update, fin_state)))
        } else {
            // Otherwise, this stream of updates should be polled again.
            ControlFlow::Continue((update, fin_state))
        };
        Poll::Ready(update)
    }
}

pub struct StreamUpdate {
    pub id: StreamId,
    pub window_update: u32,
    pub data: Bytes,
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
