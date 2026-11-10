//! The streams of a connection.

use std::{
    collections::{HashMap, hash_map::Entry},
    task::{Context, Poll},
};

use async_selector::selector::Selector;
use futures::StreamExt;
use slab::Slab;

use crate::{
    StreamId,
    buffer::Data,
    config::{RammuxConfig, RammuxRole},
    error::{ErrorKind, StreamError},
    stream::{
        self, ConnFlow, RammuxDuplex,
        handle::StreamHandle,
        updates::{StreamUpdate, StreamUpdates},
    },
    wire::header::StreamHeader,
};

/// The streams of a connection: the handles that take the inbound frames, the tasks that yield
/// the outbound ones, and the [`ConnFlow`] they share.
///
/// A stream is forgotten once it is closed on both sides. Its ID can then be reused.
/// All streams are [aborted](StreamHandle::abort) when this struct is dropped.
pub struct Streams {
    role: RammuxRole,
    config: RammuxConfig,
    /// Streams that we started.
    ///
    /// We choose their IDs, so we can use a [`Slab`]: the index of a stream is its ID, and the freed IDs are reused.
    outbound: Slab<StreamHandle>,
    /// Streams that the peer started.
    inbound: HashMap<StreamId, StreamHandle>,
    /// Polls the [`StreamUpdates`] of all streams, with the [`ConnFlow`] as the strategy.
    updates: Selector<StreamUpdates, ConnFlow>,
}

impl Streams {
    pub fn new(role: RammuxRole, config: RammuxConfig) -> Self {
        let flow =
            ConnFlow::new(config.local_recv_pool, config.remote_recv_pool).with_advance(&config);
        Self {
            role,
            config,
            outbound: Slab::new(),
            inbound: HashMap::new(),
            updates: Selector::new(flow),
        }
    }

    /// Returns the state shared by the streams.
    pub fn flow(&self) -> &ConnFlow {
        self.updates.strategy()
    }

    /// Returns the state shared by the streams.
    pub fn flow_mut(&mut self) -> &mut ConnFlow {
        self.updates.strategy_mut()
    }

    /// Returns the number of streams that the peer started.
    pub fn inbound_count(&self) -> usize {
        self.inbound.len()
    }

    /// Returns the number of streams that we started.
    pub fn outbound_count(&self) -> usize {
        self.outbound.len()
    }

    /// Starts a new stream, unless the configured limit or the IDs are exhausted.
    pub fn open_outbound(&mut self) -> Option<RammuxDuplex> {
        if self.outbound.len() >= crate::safe_cast_usize(self.config.max_outbound_streams) {
            return None;
        }
        let id = StreamId::from_slab_idx(self.outbound.vacant_key(), self.role)?;
        let (handle, updates, duplex) = stream::new(id, true, &self.config);
        self.updates.push(updates);
        self.outbound.insert(handle);
        Some(duplex)
    }

    /// Hands the stream part of an inbound frame to its stream.
    ///
    /// Returns the new stream, if the frame started one.
    pub fn receive(
        &mut self,
        header: StreamHeader,
        data: Data,
    ) -> Result<Option<RammuxDuplex>, ErrorKind> {
        let id = header.id;
        let violation = |error: &'static str| ErrorKind::Stream {
            id,
            error: StreamError(error),
        };

        let (handle, new_stream) = if id.initiated_by() == self.role {
            if header.flags.syn {
                return Err(violation(
                    "started a new stream with an ID from the wrong pool",
                ));
            }
            let handle = self
                .outbound
                .get(id.slab_idx())
                .ok_or(violation("sent a frame for an unknown stream"))?;
            (handle, None)
        } else {
            let inbound_count = self.inbound.len();
            match self.inbound.entry(id) {
                Entry::Occupied(..) if header.flags.syn => {
                    return Err(violation("started a new stream with an occupied ID"));
                },
                Entry::Occupied(entry) => (&*entry.into_mut(), None),
                Entry::Vacant(entry) if header.flags.syn => {
                    if inbound_count >= crate::safe_cast_usize(self.config.max_inbound_streams) {
                        return Err(violation(
                            "started a new stream without respecting the configured limit",
                        ));
                    }
                    let (handle, updates, duplex) = stream::new(id, false, &self.config);
                    self.updates.push(updates);
                    (&*entry.insert(handle), Some(duplex))
                },
                Entry::Vacant(..) => return Err(violation("sent a frame for an unknown stream")),
            }
        };

        let fin_state = handle
            .received(
                header.credit,
                data,
                header.flags,
                self.updates.strategy_mut(),
            )
            .map_err(|error| ErrorKind::Stream { id, error })?;
        if fin_state.is_dead() {
            self.remove(id);
        }
        Ok(new_stream)
    }

    /// Polls for the stream part of the next frame to send.
    pub fn poll_update(&mut self, cx: &mut Context<'_>) -> Poll<StreamUpdate> {
        let Poll::Ready(Some((update, fin_state))) = self.updates.poll_next_unpin(cx) else {
            return Poll::Pending;
        };
        if fin_state.is_dead() {
            self.remove(update.id);
        }
        Poll::Ready(update)
    }

    fn remove(&mut self, id: StreamId) {
        if id.initiated_by() == self.role {
            self.outbound.remove(id.slab_idx());
        } else {
            self.inbound.remove(&id);
        }
    }
}

impl Drop for Streams {
    fn drop(&mut self) {
        for handle in self.outbound.drain() {
            handle.abort();
        }
        for (_, handle) in self.inbound.drain() {
            handle.abort();
        }
    }
}
