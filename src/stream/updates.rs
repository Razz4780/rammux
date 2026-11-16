//! The tasks that yield what a stream has to send: one per direction.

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
    stream::{ConnFlow, FinState, RxFlow, StreamRef, outbound::Ctx},
    wire::{codec::encoder::OutStream, header::ControlFlags},
};

/// Yields the `WINDOW` and `FIN_READ` of a stream: what its receiving direction has to send.
///
/// The connection polls it in a selector, with the [`RxFlow`] as the strategy.
/// It is done once the stream sent `FIN_READ`. The [`FinState`] it yields is that of the whole stream.
pub(crate) struct CreditUpdates {
    id: StreamId,
    stream: StreamRef,
}

impl CreditUpdates {
    pub(super) fn new(id: StreamId, stream: StreamRef) -> Self {
        Self { id, stream }
    }
}

impl Task<RxFlow> for CreditUpdates {
    type Cont = (StreamUpdate, FinState);
    type Break = (StreamUpdate, FinState);
    type Output = (StreamUpdate, FinState);

    fn poll_progress(
        self: Pin<&mut Self>,
        flow: &mut RxFlow,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        let mut state = this.stream.lock();

        let Poll::Ready((credit, fin_read)) = state.inbound.poll_update(&mut flow.pool) else {
            // Every change that makes the direction ready wakes it while holding the lock.
            this.stream.credit_task.register(cx.waker());
            return Poll::Pending;
        };
        let update = StreamUpdate {
            id: this.id,
            credit,
            data: Bytes::new(),
            flags: ControlFlags {
                syn: std::mem::take(&mut state.syn_pending),
                fin_read,
                fin_write: false,
            },
        };
        let fin_state = state.fin_state();
        Poll::Ready(if fin_read {
            // Nothing more will be credited.
            ControlFlow::Break((update, fin_state))
        } else {
            ControlFlow::Continue((update, fin_state))
        })
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut RxFlow,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(value)
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut RxFlow,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some(value)
    }
}

/// Yields the `DATA` and `FIN_WRITE` of a stream: what its sending direction has to send.
///
/// The connection polls it in a selector, with the [`ConnFlow`] as the strategy.
/// It is done once the stream sent `FIN_WRITE`. The [`FinState`] it yields is that of the whole stream.
pub(crate) struct DataUpdates {
    id: StreamId,
    stream: StreamRef,
}

impl DataUpdates {
    pub(super) fn new(id: StreamId, stream: StreamRef) -> Self {
        Self { id, stream }
    }
}

impl Task<ConnFlow> for DataUpdates {
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

        let mut ctx = Ctx {
            flow,
            me: &this.stream,
            now: Instant::now(),
        };
        let Poll::Ready((data, fin_write)) = state.outbound.poll_update(&mut ctx) else {
            // Every change that makes the direction ready wakes it while holding the lock,
            // or happens in the connection task that is polling it now.
            this.stream.data_task.register(cx.waker());
            return Poll::Pending;
        };
        let update = StreamUpdate {
            id: this.id,
            credit: 0,
            data,
            flags: ControlFlags {
                syn: std::mem::take(&mut state.syn_pending),
                fin_read: false,
                fin_write,
            },
        };
        let fin_state = state.fin_state();
        Poll::Ready(if fin_write {
            // Nothing more will be sent.
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
        // A closed direction waits for nothing, and nobody waits for it.
        flow.pass_turn(&removed.stream);
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
