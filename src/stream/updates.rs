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

/// What a stream wants on the wire next.
///
/// Usually one frame. A stream with both a window update and payload
/// pending produces the pair, because the two have to be written
/// back to back: a stream that yields is re-enqueued at the *back* of the
/// selector, so a second frame left for the next turn waits out a full
/// rotation of every other ready stream - and where the transit window is
/// the binding constraint, that wait is a credit grant, which is a round
/// trip. Splitting the pair across turns costs ~50ms of echo latency on a
/// 25ms link.
pub(crate) struct StreamOutput {
    /// The frame to send now.
    pub first: StreamFrame,
    /// The frame to send immediately after it, if there is one.
    pub second: Option<StreamFrame>,
    /// State of the stream once both are out.
    pub fin_state: FinState,
}

impl Task<GlobalPool> for StreamUpdates {
    type Cont = StreamOutput;
    type Break = StreamOutput;
    type Output = StreamOutput;

    fn poll_progress(
        self: Pin<&mut Self>,
        global: &mut GlobalPool,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        let mut guard = this.state.lock().unwrap();

        let window_update = match guard.inbound.poll_update(global) {
            Poll::Ready((update, fin_read)) => Some((update, fin_read)),
            Poll::Pending => None,
        };
        let mut transit_blocked = false;
        let payload = match guard
            .outbound
            .poll_update(global.send_budget, &mut transit_blocked)
        {
            Poll::Ready((payload, fin_write)) => Some((payload, fin_write)),
            Poll::Pending => None,
        };

        // The window update leads. It costs no transit credit and it is
        // what lets the peer keep sending, so it must not queue behind
        // payload an exhausted transit window is holding back.
        let syn = std::mem::take(&mut this.syn);
        let (first, second) = match (window_update, payload) {
            (Some((update, fin_read)), payload) => (
                StreamFrame::WindowUpdate(StreamWindowUpdate {
                    stream_id: this.id,
                    flags: ControlFlags {
                        syn,
                        fin_read,
                        fin_write: false,
                    },
                    update,
                }),
                payload.map(|(payload, fin_write)| {
                    StreamFrame::Data(Data {
                        stream_id: this.id,
                        flags: ControlFlags {
                            syn: false,
                            fin_read: false,
                            fin_write,
                        },
                        payload: payload.into(),
                    })
                }),
            ),
            (None, Some((payload, fin_write))) => (
                StreamFrame::Data(Data {
                    stream_id: this.id,
                    flags: ControlFlags {
                        syn,
                        fin_read: false,
                        fin_write,
                    },
                    payload: payload.into(),
                }),
                None,
            ),
            (None, None) => {
                this.syn = syn;
                global.transit_blocked |= transit_blocked;
                guard.updates_poller.register(cx.waker());
                return Poll::Pending;
            },
        };

        let fin_state = guard.inbound.fin_state().and(guard.outbound.fin_state());
        drop(guard);

        let output = StreamOutput {
            first,
            second,
            fin_state,
        };
        Poll::Ready(if fin_state.sent {
            // If we sent both fins, this stream of updates is done.
            ControlFlow::Break(output)
        } else {
            // Otherwise, this stream of updates should be polled again.
            ControlFlow::Continue(output)
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
