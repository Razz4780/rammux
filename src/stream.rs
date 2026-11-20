//! Types for working with virtual rammux streams.

use std::sync::{Arc, Mutex, MutexGuard};

use futures::task::AtomicWaker;
use tokio::time::Instant;

use crate::{
    StreamId,
    config::RammuxConfig,
    pin_list::{Linked, Node},
    stream::{
        handle::StreamHandle,
        inbound::InboundTraffic,
        outbound::OutboundTraffic,
        updates::{CreditUpdates, DataUpdates},
    },
};

pub(crate) use conn_flow::{ConnFlow, RxFlow};
pub use duplex::{RammuxDuplex, RammuxSink, RammuxStream, SinkClose};

pub use crate::error::StreamError;

mod conn_flow;
mod duplex;
pub(crate) mod handle;
mod inbound;
mod outbound;
#[cfg(test)]
mod test;
#[cfg(test)]
pub(crate) mod test_support;
mod turns;
pub(crate) mod updates;
mod waker;

/// Creates the state of a new stream, and the handles to it.
///
/// The first frame of an outbound stream carries `SYN`.
pub(crate) fn new(
    id: StreamId,
    is_outbound: bool,
    config: &RammuxConfig,
) -> (StreamHandle, CreditUpdates, DataUpdates, RammuxDuplex) {
    let shared = Arc::new(StreamShared {
        credit_task: AtomicWaker::new(),
        data_task: AtomicWaker::new(),
        turn: Node::new(),
        state: Mutex::new(StreamState {
            inbound: InboundTraffic::new(config.local_recv_window.get()),
            outbound: OutboundTraffic::new(config.frame_limit, config.remote_recv_window.get()),
            syn_pending: is_outbound,
        }),
    });

    (
        StreamHandle(shared.clone()),
        CreditUpdates::new(id, shared.clone()),
        DataUpdates::new(id, shared.clone()),
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
/// - the [`CreditUpdates`] and [`DataUpdates`] tasks, which the connection polls for what the stream has to send.
///
/// The two directions of a stream are independent state machines, [`InboundTraffic`] and
/// [`OutboundTraffic`], and each has its own task. What the streams of a connection share
/// is the [`RxFlow`] of the receiving directions and the [`ConnFlow`] of the sending ones.
pub(crate) struct StreamShared {
    /// Woken when the receiving direction has something to send, see [`CreditUpdates`].
    credit_task: AtomicWaker,
    /// Woken when the sending direction has something to send, see [`DataUpdates`].
    ///
    /// It lives outside of [`Self::state`], so the turn queue can wake a stream without taking its lock.
    data_task: AtomicWaker,
    /// Place of the stream in the turn queue, see [`turns::Turns`].
    turn: Node<StreamShared>,
    state: Mutex<StreamState>,
}

// SAFETY: the node is a field of the stream.
unsafe impl Linked for StreamShared {
    fn node(&self) -> &Node<Self> {
        &self.turn
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
    /// Whether the stream still has to announce itself with `SYN`.
    ///
    /// The task that builds the first frame of the stream takes it, whichever direction that is.
    syn_pending: bool,
}

impl StreamState {
    /// Returns the `FIN_*` state of the whole stream.
    fn fin_state(&self) -> FinState {
        self.inbound.fin_state().and(self.outbound.fin_state())
    }

    /// Returns the byte counts of the stream.
    fn stats(&self) -> StreamStats {
        StreamStats {
            unacked: self.outbound.unacked(),
            unsent: self.outbound.unsent(),
            unread: self.inbound.unread(),
            last_ack: self.outbound.last_ack(),
        }
    }
}

/// Statistics of a single rammux stream.
///
/// With [`Self::unacked`] and [`Self::last_ack`], the application can decide that the peer's reader stalls,
/// and close the sink with [`SinkClose::Reset`]. Not that:
/// - [`Self::unacked`] counts the bytes that are still on their way, too.
/// - The peer acks bytes in steps. A small count does not have to move, ever.
/// - On a slow connection, or on one that many streams share, a stream whose reader keeps up also goes
///   for long between two acks. A limit has to allow for the connection.
///
/// ```
/// # use std::time::Duration;
/// # use rammux::stream::{RammuxSink, SinkClose};
/// # fn give_up(mut sink: RammuxSink) {
/// let stats = sink.stats();
/// let waited = stats.last_ack.map(|at| at.elapsed());
/// if stats.unacked >= 1024 * 1024 && waited.is_some_and(|waited| waited > Duration::from_secs(30)) {
///     sink.on_close(SinkClose::Reset);
///     drop(sink);
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamStats {
    /// Bytes that this side sent, and that the peer's reader did not ack yet.
    /// They count against the buffers of the peer, wherever they are.
    ///
    /// Once the peer stopped reading, nothing is unacked.
    pub unacked: u64,
    /// Bytes that the application wrote, and that the connection did not send yet.
    ///
    /// Once writing ended in a way that discards them, nothing is unsent.
    pub unsent: u64,
    /// Bytes that this side received, and that the application did not read yet.
    ///
    /// Once the application stopped reading, or the peer reset the stream, nothing is unread.
    pub unread: u64,
    /// When the peer last acked bytes of the stream: when its last window update arrived.
    /// [`None`] before the first ack.
    pub last_ack: Option<Instant>,
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
