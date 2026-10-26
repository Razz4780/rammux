use std::collections::HashMap;

use async_selector::selector::Selector;
use slab::Slab;

use crate::{
    StreamId,
    clock::Clock,
    codec::RammuxCodec,
    connection::{downgrade::Downgraded, pings::PingSchedule},
    error::ErrorKind,
    global_pool::GlobalPool,
    header::Pong,
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

pub struct Active<IO> {
    pub codec: RammuxCodec<IO>,
    pub streams: ActiveStreams,
    pub selector: Selector<StreamUpdates, GlobalPool>,
    /// Decides when to send `PING`s.
    pub pings: PingSchedule,
    /// The answer to the latest `PING` we read, if it is not written yet. The next outgoing frame takes it.
    ///
    /// Only the latest `PING` is answered; a newer one overwrites an unanswered one. Why:
    /// 1. The memory is O(1), with no cap and no read stall: a flood of `PING`s costs only CPU, like
    ///    any flood of frames.
    /// 2. An answer that waits in a queue carries our queueing in the peer's back-delay sample.
    ///    The latest `PING` is the freshest sample.
    /// 3. Nothing on the sender's side needs one `PONG` per `PING`: `rx_bytes` is cumulative, and every
    ///    `PONG` is an independent sample.
    /// 4. No deadlock: a cap on owed `PONG`s could stop both sides from reading at the same time.
    pub pong: Option<Pong>,
    /// The private clock of this side.
    pub clock: Clock,
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
