//! Types for working with virtual rammux streams.

use std::sync::{Arc, Mutex, MutexGuard};

use futures::task::AtomicWaker;

use crate::{
    StreamId,
    config::RammuxConfig,
    pin_list::{Linked, Node},
    stream::{
        handle::StreamHandle, inbound::InboundTraffic, outbound::OutboundTraffic,
        updates::StreamUpdates,
    },
};

pub(crate) use conn_flow::ConnFlow;
pub use duplex::{RammuxDuplex, RammuxSink, RammuxStream};

#[cfg(test)]
mod accounting_test;
mod conn_flow;
mod duplex;
pub(crate) mod handle;
mod inbound;
mod outbound;
#[cfg(test)]
mod test;
mod turns;
#[cfg(test)]
mod turns_test;
pub(crate) mod updates;
mod waker;

/// Creates the state of a new stream, and the handles to it.
pub(crate) fn new(
    id: StreamId,
    is_outbound: bool,
    config: &RammuxConfig,
) -> (StreamHandle, StreamUpdates, RammuxDuplex) {
    let shared = Arc::new(StreamShared {
        updates: AtomicWaker::new(),
        turn: Node::new(),
        budget: Node::new(),
        state: Mutex::new(StreamState {
            inbound: InboundTraffic::new(config.local_recv_window.get()),
            outbound: OutboundTraffic::new(config.frame_limit, config.remote_recv_window.get()),
        }),
    });

    (
        StreamHandle(shared.clone()),
        StreamUpdates::new(id, is_outbound, shared.clone()),
        RammuxDuplex::new(id, shared),
    )
}

/// A stream, as the turn queue and the handles know it.
///
/// Its identity is the identity of the stream: IDs are reused, this is not.
pub(crate) type StreamRef = Arc<StreamShared>;

/// State of a virtual stream, shared through an [`Arc`] by:
/// - the application, which holds the [`RammuxSink`] and [`RammuxStream`] halves of a [`RammuxDuplex`],
/// - the connection, which hands the parts of inbound frames to the [`StreamHandle`],
/// - the [`StreamUpdates`] task, which the connection polls for what the stream has to send.
///
/// The two directions of a stream are independent state machines, [`InboundTraffic`] and
/// [`OutboundTraffic`]. What all streams of a connection share is the [`ConnFlow`].
pub(crate) struct StreamShared {
    /// Woken when the stream has something to send, see [`StreamUpdates`].
    ///
    /// It lives outside of [`Self::state`], so the turn queue can wake a stream without taking its lock.
    updates: AtomicWaker,
    /// Place of the stream in the turn queue, see [`turns::Turns`].
    turn: Node<StreamShared>,
    /// Place of the stream in the queue for the budget of the advance, see [`turns::BudgetWaiters`].
    budget: Node<StreamShared>,
    state: Mutex<StreamState>,
}

// SAFETY: the node is a field of the stream.
unsafe impl Linked for StreamShared {
    fn node(&self) -> &Node<Self> {
        &self.turn
    }
}

// SAFETY: the node is a field of the stream.
unsafe impl Linked<turns::BudgetLink> for StreamShared {
    fn node(&self) -> &Node<Self> {
        &self.budget
    }
}

impl StreamShared {
    /// Locks the state of the stream.
    ///
    /// # Panics
    ///
    /// Panics if another user of the stream panicked while holding the lock.
    fn lock(&self) -> MutexGuard<'_, StreamState> {
        self.state.lock().expect("the stream state is poisoned")
    }

    /// Locks the state of the stream, unless another user of the stream panicked while holding the lock.
    fn try_lock(&self) -> Option<MutexGuard<'_, StreamState>> {
        self.state.lock().ok()
    }
}

/// The two directions of a stream.
struct StreamState {
    inbound: InboundTraffic,
    outbound: OutboundTraffic,
}

impl StreamState {
    /// Returns the `FIN_*` state of the whole stream.
    fn fin_state(&self) -> FinState {
        self.inbound.fin_state().and(self.outbound.fin_state())
    }
}

/// Tells which `FIN_*` frames of a stream, or of one of its directions, were sent and received.
///
/// For the inbound direction, we send `FIN_READ` and receive `FIN_WRITE`.
/// For the outbound direction, we send `FIN_WRITE` and receive `FIN_READ`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FinState {
    pub(crate) sent: bool,
    pub(crate) received: bool,
}

impl FinState {
    /// Returns whether all `FIN_*`s were sent and received: the stream is closed on both sides.
    pub(crate) fn is_dead(self) -> bool {
        self.sent && self.received
    }

    fn and(self, other: Self) -> Self {
        Self {
            sent: self.sent && other.sent,
            received: self.received && other.received,
        }
    }
}
