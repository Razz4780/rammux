use std::{
    ops::ControlFlow,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::time::Instant;

use async_selector::{
    selector::{BorrowedMut, Removed},
    task::Task,
};
use bytes::Bytes;

use crate::{
    StreamId,
    codec::encoder::OutStream,
    global_pool::GlobalPool,
    header::ControlFlags,
    stream::{FinState, SharedStreamState, outbound::Ctx},
};

pub(crate) struct StreamUpdates {
    pub(super) id: StreamId,
    pub(super) syn: bool,
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
        let mut update = StreamUpdate {
            id: this.id,
            window_update: 0,
            data: Default::default(),
            flags: ControlFlags::default(),
        };
        let mut is_pending = true;

        let mut guard = this.state.lock().unwrap();
        let SharedStreamState {
            inbound,
            outbound,
            updates_poller,
        } = &mut *guard;
        // Credit and FIN_READ never wait for data, or for a turn.
        if let Poll::Ready((window_update, fin_read)) = inbound.poll_update(global) {
            update.window_update = window_update;
            update.flags.fin_read = fin_read;
            is_pending = false;
        }
        let mut ctx = Ctx {
            global,
            me: &this.state,
            poller: updates_poller,
            now: Instant::now(),
        };
        if let Poll::Ready((data, fin_write)) = outbound.poll_update(&mut ctx) {
            update.data = data;
            update.flags.fin_write = fin_write;
            is_pending = false;
        }
        if is_pending {
            updates_poller.register(cx.waker());
            return Poll::Pending;
        }
        let inbound_fin = inbound.fin_state();
        let outbound_fin = outbound.fin_state();
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
        removed: Removed<Self>,
        global: &mut GlobalPool,
        value: Self::Break,
    ) -> Option<Self::Output> {
        // A closed stream waits for nothing, and nobody waits for it.
        let mut guard = removed.state.lock().unwrap();
        guard.outbound.leave_turns(global, &removed.state);
        drop(guard);
        Some(value)
    }
}

pub struct StreamUpdate {
    pub id: StreamId,
    pub window_update: u32,
    pub data: Bytes,
    pub flags: ControlFlags,
}

impl From<StreamUpdate> for OutStream {
    fn from(value: StreamUpdate) -> Self {
        Self {
            id: value.id,
            flags: value.flags,
            credit: value.window_update,
            data: value.data,
        }
    }
}
