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

use crate::{
    StreamId,
    global_pool::GlobalPool,
    header::ControlFlags,
    stream::{FinState, SharedStreamState},
    transport::{Data, StreamFrame, StreamWindowUpdate},
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
    type Cont = (StreamFrame, FinState);
    type Break = (StreamFrame, FinState);
    type Output = (StreamFrame, FinState);

    /// Produces the stream's next frame, at most one per poll.
    ///
    /// Payload goes first. A stream that yields here is re-enqueued at
    /// the *back* of the selector, so whichever frame loses this choice
    /// waits for every other ready stream - and on a link where the
    /// transit window is the binding constraint, that wait is a credit
    /// grant, which is a round trip. Putting the window update first
    /// costs exactly the streams that care about latency: a request's
    /// reply queues behind a full rotation of the bulk senders.
    ///
    /// Both directions still get served, because payload the transit
    /// window cannot carry reports pending and the update goes in its
    /// place. A sender that never yields is impossible: it runs out of
    /// transit credit or of the peer's stream window, and its update
    /// leaves on the turn after that.
    fn poll_progress(
        self: Pin<&mut Self>,
        global: &mut GlobalPool,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        let mut guard = this.state.lock().unwrap();

        let mut flags = ControlFlags::default();
        let mut transit_blocked = false;
        let outbound = guard
            .outbound
            .poll_update(global.send_budget, &mut transit_blocked);
        let frame = if let Poll::Ready((payload, fin_write)) = outbound {
            flags.fin_write = fin_write;
            Some(StreamFrame::Data(Data {
                stream_id: this.id,
                flags,
                payload: payload.into(),
            }))
        } else if let Poll::Ready((update, fin_read)) = guard.inbound.poll_update(global) {
            flags.fin_read = fin_read;
            Some(StreamFrame::WindowUpdate(StreamWindowUpdate {
                stream_id: this.id,
                flags,
                update,
            }))
        } else {
            global.transit_blocked |= transit_blocked;
            None
        };

        let Some(mut frame) = frame else {
            guard.updates_poller.register(cx.waker());
            return Poll::Pending;
        };

        // The first frame of a stream is the one that opens it.
        let syn = std::mem::take(&mut this.syn);
        match &mut frame {
            StreamFrame::Data(data) => data.flags.syn = syn,
            StreamFrame::WindowUpdate(update) => update.flags.syn = syn,
        }

        let fin_state = guard.inbound.fin_state().and(guard.outbound.fin_state());
        drop(guard);

        Poll::Ready(if fin_state.sent {
            // If we sent both fins, this stream of updates is done.
            ControlFlow::Break((frame, fin_state))
        } else {
            // Otherwise, this stream of updates should be polled again.
            ControlFlow::Continue((frame, fin_state))
        })
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
