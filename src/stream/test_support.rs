//! Helpers that the tests of the streams share.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use async_selector::selector::Selector;
use futures::{StreamExt, task::ArcWake};

use crate::{
    buffer::Data,
    error::StreamViolation,
    stream::{
        ConnFlow, FinState, RxFlow,
        handle::StreamHandle,
        updates::{CreditUpdates, DataUpdates, StreamUpdate},
    },
    wire::header::ControlFlags,
};

/// A waker that sets the flag.
pub struct Flag(pub AtomicBool);

impl ArcWake for Flag {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::SeqCst);
    }
}

/// The flags of a frame that ends nothing but what the arguments say.
pub fn fins(fin_write: bool, fin_read: bool) -> ControlFlags {
    ControlFlags {
        syn: false,
        fin_read,
        fin_write,
        abort: false,
    }
}

/// The two selectors of a connection, which poll the tasks of the streams.
pub struct Selectors {
    pub credit: Selector<CreditUpdates, RxFlow>,
    pub data: Selector<DataUpdates, ConnFlow>,
}

impl Selectors {
    pub fn new(rx: RxFlow, tx: ConnFlow) -> Self {
        Self {
            credit: Selector::new(rx),
            data: Selector::new(tx),
        }
    }

    /// Hands the parts of an inbound frame to the stream, with the shared state of these selectors.
    pub fn received(
        &mut self,
        handle: &StreamHandle,
        credit: Option<NonZeroU32>,
        data: Data,
        flags: ControlFlags,
    ) -> Result<FinState, StreamViolation> {
        handle.received(
            credit,
            data,
            flags,
            self.data.strategy_mut(),
            self.credit.strategy_mut(),
        )
    }

    pub fn rx_pool_used(&self) -> u64 {
        self.credit.strategy().pool.used()
    }

    /// Polls until the selectors have nothing more, and nobody woke them up, credit first like the connection.
    /// Hands every update to `each`.
    pub fn drain(&mut self, mut each: impl FnMut(StreamUpdate, FinState)) {
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = futures::task::waker(flag.clone());
        let mut cx = Context::from_waker(&waker);
        loop {
            flag.0.store(false, Ordering::SeqCst);
            let polled = match self.credit.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(yielded)) => Poll::Ready(Some(yielded)),
                _ => self.data.poll_next_unpin(&mut cx),
            };
            match polled {
                Poll::Ready(Some((update, fin_state))) => each(update, fin_state),
                Poll::Ready(None) => break,
                Poll::Pending if flag.0.load(Ordering::SeqCst) => {},
                Poll::Pending => break,
            }
        }
    }
}
