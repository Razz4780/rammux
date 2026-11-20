//! The queue of streams that wait for their turn to borrow from the peer's pool.

use std::ptr;

use crate::{
    pin_list::PinList,
    stream::{StreamRef, StreamShared},
};

/// Streams that wait for their turn to borrow from the peer's pool, in the order of their turns.
///
/// A stream that needs the pool and cannot get it joins at the back. Only the stream at the front may borrow,
/// so free room never lets a stream skip the queue. A stream leaves the queue when it borrowed
/// (and joins again at the back if it has more to send), when it stops needing the pool, and when it closes.
///
/// A turn carries no byte count. The allowance of a stream keeps decaying while it waits, so the stream
/// works out the amount when its turn comes. A semaphore that fixed the amount at the time of the request
/// could grant a stalled stream more than its allowance, and that stream would hold up the queue.
///
/// The queue is intrusive: a stream is linked through the node in its [`StreamShared`],
/// so joining and leaving are O(1) and never allocate.
///
/// A stream joins only the queue of its own connection, the one in the [`ConnFlow`](super::ConnFlow)
/// that its handles are given. The node of a stream is therefore never in another list,
/// which is what the unsafe methods of [`PinList`] require.
#[derive(Default)]
pub struct Turns {
    queue: PinList<StreamShared>,
}

impl Turns {
    /// Returns the number of waiting streams.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns whether the stream may borrow now: nobody waits, or the stream is first in line.
    pub fn has_turn(&self, stream: &StreamShared) -> bool {
        self.queue
            .front()
            .is_none_or(|front| ptr::eq(front, stream))
    }

    /// Puts the stream at the back of the queue, unless it is in the queue already.
    pub fn join(&mut self, stream: &StreamRef) {
        // SAFETY: the stream joins only the queue of its own connection.
        unsafe { self.queue.push_back(stream) };
    }

    /// Takes the stream out of the queue, wherever it is.
    ///
    /// Returns whether the stream was in the queue.
    pub fn leave(&mut self, stream: &StreamRef) -> bool {
        // SAFETY: the stream joins only the queue of its own connection.
        unsafe { self.queue.remove(stream) }
    }

    /// Wakes the [`DataUpdates`](super::updates::DataUpdates) of the stream that is first in line, if any.
    pub fn wake_front(&self) {
        if let Some(front) = self.queue.front() {
            front.data_task.wake();
        }
    }

    /// Returns whether the stream waits in the queue.
    #[cfg(test)]
    pub fn contains(&self, stream: &StreamShared) -> bool {
        // SAFETY: the stream joins only the queue of its own connection.
        unsafe { self.queue.contains(stream) }
    }

    /// Returns the waiting streams, first in line first.
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = &StreamShared> {
        self.queue.iter()
    }
}
