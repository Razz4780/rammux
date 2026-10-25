//! Types for working with virtual rammux streams.

use std::sync::{Arc, Mutex};

use crate::{
    StreamId,
    config::RammuxConfig,
    stream::{
        handle::StreamHandle, inbound::InboundTraffic, outbound::OutboundTraffic,
        updates::StreamUpdates, waker::WakerSlot,
    },
};

pub use duplex::{RammuxDuplex, RammuxSink, RammuxStream};

mod duplex;
pub(crate) mod handle;
mod inbound;
pub(crate) mod outbound;
#[cfg(test)]
mod test;
pub(crate) mod updates;
pub(crate) mod waker;

pub(crate) fn new(
    id: StreamId,
    is_outbound: bool,
    config: &RammuxConfig,
) -> (StreamHandle, StreamUpdates, RammuxDuplex) {
    let state = SharedStreamState {
        inbound: InboundTraffic::new(config.local_recv_window.get()),
        outbound: OutboundTraffic::new(config.frame_limit, config.remote_recv_window.get()),
        updates_poller: Default::default(),
    };
    let state = Arc::new(Mutex::new(state));

    (
        StreamHandle(state.clone()),
        StreamUpdates {
            id,
            syn: is_outbound,
            state: state.clone(),
        },
        RammuxDuplex {
            sink: RammuxSink {
                id,
                state: Some(state.clone()),
            },
            stream: RammuxStream {
                id,
                state: Some(state),
            },
        },
    )
}

/// Reference to the state of a virtual stream.
///
/// Its identity is the identity of the stream: IDs are reused, this is not.
pub(crate) type StreamRef = Arc<Mutex<SharedStreamState>>;

/// State of a virtual stream, shared under [`Mutex`] by [`StreamHandle`], [`StreamUpdates`] and [`RammuxDuplex`].
pub(crate) struct SharedStreamState {
    /// State of the inbound traffic direction.
    inbound: InboundTraffic,
    /// State of the outbound traffic direction.
    outbound: OutboundTraffic,
    /// Waker for the task that polls [`StreamUpdates`].
    pub(crate) updates_poller: WakerSlot,
}

/// Represents the current `FIN_*` state of a virtual stream or one of its traffic directions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FinState {
    sent: bool,
    received: bool,
}

impl FinState {
    /// Returns whether all fins were sent and received.
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
