//! Lifecycle states of a [`RammuxConnection`](crate::connection::RammuxConnection).

use std::collections::HashMap;

use async_selector::selector::Selector;
use slab::Slab;

use crate::{
    StreamId,
    codec::RammuxCodec,
    connection::downgrade::Downgraded,
    error::ErrorKind,
    global_pool::GlobalPool,
    stream::{handle::StreamHandle, updates::StreamUpdates},
};

/// Inner state of a [`RammuxConnection`](super::RammuxConnection).
#[allow(clippy::large_enum_variant)]
pub enum ConnState<IO> {
    /// Connection is active.
    Active(Active<IO>),
    /// Connection has failed.
    Poisoned,
    /// Connection was downgraded.
    Downgraded,
}

impl<IO> ConnState<IO> {
    pub fn active_mut(&mut self) -> Result<&mut Active<IO>, ErrorKind> {
        match self {
            Self::Active(active) => Ok(active),
            Self::Poisoned => Err(ErrorKind::Poisoned),
            Self::Downgraded => Err(ErrorKind::AlreadyDowngraded),
        }
    }

    pub fn active(&self) -> Result<&Active<IO>, ErrorKind> {
        match self {
            Self::Active(active) => Ok(active),
            Self::Poisoned => Err(ErrorKind::Poisoned),
            Self::Downgraded => Err(ErrorKind::AlreadyDowngraded),
        }
    }

    pub fn downgrade(&mut self, term_received: bool) -> Result<Downgraded<IO>, ErrorKind> {
        match std::mem::replace(self, Self::Downgraded) {
            Self::Active(active) => Ok(Downgraded::new(active.codec, term_received)),
            Self::Poisoned => {
                *self = Self::Poisoned;
                Err(ErrorKind::Poisoned)
            },
            Self::Downgraded => Err(ErrorKind::AlreadyDowngraded),
        }
    }
}

/// Everything a live connection owns.
pub struct Active<IO> {
    /// Framing over the IO transport.
    pub codec: RammuxCodec<IO>,
    /// Streams this connection is serving.
    pub streams: ActiveStreams,
    /// Round-robin over the streams that have something to send, with the
    /// connection-wide state they share as its polling strategy.
    pub selector: Selector<StreamUpdates, GlobalPool>,
}

/// Stores active rammux streams.
///
/// All stored streams are aborted with [`StreamHandle::try_abort`] when this struct is dropped.
#[derive(Default)]
pub struct ActiveStreams {
    /// Outbound streams.
    ///
    /// We control the IDs of outbound streams, so we can use a [`Slab`] allocator.
    /// It gives us two things for free:
    /// 1. Faster access by ID.
    /// 2. Automatic reuse of freed IDs.
    pub outbound: Slab<StreamHandle>,
    /// Inbound streams.
    ///
    /// We do not control IDs of inbound streams, so we use a plain [`HashMap`].
    pub inbound: HashMap<StreamId, StreamHandle>,
}

impl Drop for ActiveStreams {
    fn drop(&mut self) {
        for stream in self.outbound.drain() {
            stream.try_abort();
        }
        for (_, stream) in self.inbound.drain() {
            stream.try_abort();
        }
    }
}
