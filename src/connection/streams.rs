//! The streams of a connection.

use std::{
    collections::{HashMap, hash_map::Entry},
    task::{Context, Poll},
    time::Duration,
};

use async_selector::selector::Selector;
use futures::StreamExt;
use slab::Slab;

use crate::{
    StreamId,
    buffer::Data,
    config::{RammuxConfig, RammuxRole},
    error::{ErrorKind, StreamError, StreamViolation},
    flow::Srtt,
    stream::{
        self, ConnFlow, FinState, RammuxDuplex, RxFlow,
        handle::StreamHandle,
        updates::{CreditUpdates, DataUpdates, StreamUpdate},
    },
    wire::header::StreamHeader,
};

/// The streams of a connection: the handles that take the inbound frames, the tasks that yield
/// the outbound ones, and the [`RxFlow`] and [`ConnFlow`] they share.
///
/// A stream is forgotten once it is closed on both sides. Its ID can then be reused.
/// The streams that are left are [aborted](StreamHandle::abort) when the connection ends:
/// with [`Self::end`], or with [`StreamError::ConnectionDropped`] when this struct is dropped.
pub struct Streams {
    role: RammuxRole,
    config: RammuxConfig,
    /// Streams that we started.
    ///
    /// We choose their IDs, so we can use a [`Slab`]: the index of a stream is its ID, and the freed IDs are reused.
    outbound: Slab<StreamHandle>,
    /// Streams that the peer started.
    inbound: HashMap<StreamId, StreamHandle>,
    /// Polls the [`CreditUpdates`] of all streams, with the [`RxFlow`] as the strategy.
    credit: Selector<CreditUpdates, RxFlow>,
    /// Polls the [`DataUpdates`] of all streams, with the [`ConnFlow`] as the strategy.
    data: Selector<DataUpdates, ConnFlow>,
}

impl Streams {
    pub fn new(role: RammuxRole, config: RammuxConfig) -> Self {
        let tx = ConnFlow::new(config.remote_recv_pool).with_advance(&config);
        let rx = RxFlow::new(config.local_recv_pool);
        Self {
            role,
            config,
            outbound: Slab::new(),
            inbound: HashMap::new(),
            credit: Selector::new(rx),
            data: Selector::new(tx),
        }
    }

    /// Returns the state shared by the sending directions of the streams.
    pub fn tx_flow(&self) -> &ConnFlow {
        self.data.strategy()
    }

    /// Returns the state shared by the receiving directions of the streams.
    pub fn rx_flow(&self) -> &RxFlow {
        self.credit.strategy()
    }

    /// Returns the smoothed round trip time of the connection.
    pub fn srtt(&self) -> Srtt {
        self.tx_flow().srtt
    }

    /// Folds a round trip sample into the smoothed round trip time of both directions.
    pub fn observe_rtt(&mut self, rtt: Duration) {
        self.data.strategy_mut().srtt.observe(rtt);
        self.credit.strategy_mut().srtt.observe(rtt);
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
        if self.outbound.len() >= usize::from(self.config.max_outbound_streams) {
            return None;
        }
        let id = StreamId::from_slab_idx(self.outbound.vacant_key(), self.role)?;
        let (handle, credit, data, duplex) = stream::new(id, true, &self.config);
        self.credit.push(credit);
        self.data.push(data);
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
            error: StreamViolation(error),
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
                    if inbound_count >= usize::from(self.config.max_inbound_streams) {
                        return Err(violation(
                            "started a new stream without respecting the configured limit",
                        ));
                    }
                    let (handle, credit, data, duplex) = stream::new(id, false, &self.config);
                    self.credit.push(credit);
                    self.data.push(data);
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
                self.data.strategy_mut(),
                self.credit.strategy_mut(),
            )
            .map_err(|error| ErrorKind::Stream { id, error })?;
        if fin_state.is_dead() {
            self.remove(id);
        }
        Ok(new_stream)
    }

    /// Polls for the stream part of the next frame that carries `WINDOW` or `FIN_READ`.
    pub fn poll_credit(&mut self, cx: &mut Context<'_>) -> Poll<StreamUpdate> {
        // The streams that the timer wakes are polled in this pass.
        self.credit.strategy_mut().settle.poll(cx);
        let polled = self.credit.poll_next_unpin(cx);
        // A stream may have armed the timer in this pass, and the timer wakes only the task that polled it.
        self.credit.strategy_mut().settle.poll(cx);
        self.take_update(polled)
    }

    /// Polls for the stream part of the next frame that carries `DATA` or `FIN_WRITE`.
    pub fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<StreamUpdate> {
        let polled = self.data.poll_next_unpin(cx);
        self.take_update(polled)
    }

    /// Takes an update that a selector yielded, and forgets its stream if the update closes it.
    ///
    /// A selector without tasks yields [`None`]: there is nothing to send.
    fn take_update(
        &mut self,
        polled: Poll<Option<(StreamUpdate, FinState)>>,
    ) -> Poll<StreamUpdate> {
        let Poll::Ready(Some((update, fin_state))) = polled else {
            return Poll::Pending;
        };
        if fin_state.is_dead() {
            self.remove(update.id);
        }
        Poll::Ready(update)
    }

    /// Aborts all streams, because the connection is gone. Their applications get the `reason`.
    pub fn end(&mut self, reason: StreamError) {
        for handle in self.outbound.drain() {
            handle.abort(reason);
        }
        for (_, handle) in self.inbound.drain() {
            handle.abort(reason);
        }
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
        self.end(StreamError::ConnectionDropped);
    }
}
