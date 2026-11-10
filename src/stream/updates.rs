//! The task that yields what a stream has to send.

use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use async_selector::{
    selector::{BorrowedMut, Removed},
    task::Task,
};
use bytes::Bytes;
use tokio::time::Instant;

use crate::{
    StreamId,
    stream::{ConnFlow, FinState, StreamRef, outbound::Ctx},
    wire::{codec::encoder::OutStream, header::ControlFlags},
};

/// Yields the stream parts of the frames that a stream has to send.
///
/// The connection polls it in a selector, with the [`ConnFlow`] as the strategy.
/// It is done once the stream sent both `FIN_READ` and `FIN_WRITE`.
pub(crate) struct StreamUpdates {
    id: StreamId,
    /// Whether the next update carries `SYN`.
    syn: bool,
    stream: StreamRef,
}

impl StreamUpdates {
    pub(super) fn new(id: StreamId, syn: bool, stream: StreamRef) -> Self {
        Self { id, syn, stream }
    }
}

impl Task<ConnFlow> for StreamUpdates {
    type Cont = (StreamUpdate, FinState);
    type Break = (StreamUpdate, FinState);
    type Output = (StreamUpdate, FinState);

    fn poll_progress(
        self: Pin<&mut Self>,
        flow: &mut ConnFlow,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        let mut state = this.stream.lock();

        // Credit and `FIN_READ` never wait for data, or for a turn.
        let inbound = state.inbound.poll_update(&mut flow.rx_pool);
        let mut ctx = Ctx {
            flow,
            me: &this.stream,
            now: Instant::now(),
        };
        let outbound = state.outbound.poll_update(&mut ctx);
        if inbound.is_pending() && outbound.is_pending() {
            // Every change that makes the stream ready wakes it while holding the lock,
            // or happens in the connection task that is polling it now.
            this.stream.updates.register(cx.waker());
            return Poll::Pending;
        }
        let fin_state = state.fin_state();
        drop(state);

        let (credit, fin_read) = match inbound {
            Poll::Ready(update) => update,
            Poll::Pending => (0, false),
        };
        let (data, fin_write) = match outbound {
            Poll::Ready(update) => update,
            Poll::Pending => (Bytes::new(), false),
        };
        let update = StreamUpdate {
            id: this.id,
            credit,
            data,
            flags: ControlFlags {
                syn: std::mem::take(&mut this.syn),
                fin_read,
                fin_write,
            },
        };
        Poll::Ready(if fin_state.sent {
            // Both `FIN_*`s are sent, nothing more will come.
            ControlFlow::Break((update, fin_state))
        } else {
            ControlFlow::Continue((update, fin_state))
        })
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut ConnFlow,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(value)
    }

    fn transform_break(
        removed: Removed<Self>,
        flow: &mut ConnFlow,
        value: Self::Break,
    ) -> Option<Self::Output> {
        // A closed stream waits for nothing, and nobody waits for it.
        flow.leave_queues(&removed.stream);
        Some(value)
    }
}

/// The stream part of a frame to send.
pub struct StreamUpdate {
    pub id: StreamId,
    /// Bytes returned to the peer's window, 0 for none.
    pub credit: u32,
    pub data: Bytes,
    pub flags: ControlFlags,
}

impl From<StreamUpdate> for OutStream {
    fn from(update: StreamUpdate) -> Self {
        Self {
            id: update.id,
            flags: update.flags,
            credit: update.credit,
            data: update.data,
        }
    }
}
