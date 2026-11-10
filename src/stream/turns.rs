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
/// The queue is intrusive: a stream is linked through the node in its [`StreamShared`],
/// so joining and leaving are O(1) and never allocate.
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
        self.queue.push_back(stream);
    }

    /// Takes the stream out of the queue, wherever it is.
    ///
    /// Returns whether the stream was in the queue.
    pub fn leave(&mut self, stream: &StreamRef) -> bool {
        self.queue.remove(stream)
    }

    /// Wakes the stream that is first in line, if any.
    pub fn wake_front(&self) {
        if let Some(front) = self.queue.front() {
            front.updates.wake();
        }
    }

    /// Returns whether the stream waits in the queue.
    #[cfg(test)]
    pub fn contains(&self, stream: &StreamShared) -> bool {
        self.queue.contains(stream)
    }

    /// Returns the waiting streams, first in line first.
    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = &StreamShared> {
        self.queue.iter()
    }
}
