use std::{collections::VecDeque, time::Duration};

use crate::{
    stream::{StreamRef, waker::WakerSlot},
    transit::{ALLOWANCE_RTTS, SRTT_GAIN, SRTT_MIN},
};

/// Connection-level state shared by all streams of a rammux connection.
///
/// It lives in the strategy of the stream selector, so every stream gets it when it is polled,
/// and the connection gets it when it handles an inbound frame.
#[derive(Default)]
pub struct GlobalPool {
    /// Smoothed round-trip time of the connection, [`None`] before the first sample.
    ///
    /// Never zero, see [`SRTT_MIN`].
    pub srtt: Option<Duration>,
    /// Bytes of `DATA` written to the transport. Wraps.
    pub tx_bytes: u32,
    /// Bytes of `DATA` read from the transport, including discarded ones. Wraps.
    pub rx_bytes: u32,
    /// Size of our receive pool, `P_rx`.
    pub rx_pool: u64,
    /// Bytes of our receive pool used by the inbound streams, `U_rx`.
    ///
    /// Sum over the streams of `max(0, unacked - B)`.
    pub rx_used: u64,
    /// Size of the receive pool of the peer, `P_tx`.
    pub tx_pool: u64,
    /// Bytes of the peer's pool that we use, `U_tx`.
    ///
    /// Sum over our sending directions of `max(0, unacked - B)`.
    pub tx_used: u64,
    /// Streams that wait for shared room, in the order of their turns.
    ///
    /// Each queued stream has its `queued` flag set.
    turns: VecDeque<StreamRef>,
}

impl GlobalPool {
    /// Creates the state of a new connection.
    pub fn new(rx_pool: u32, tx_pool: u32) -> Self {
        Self {
            rx_pool: u64::from(rx_pool),
            tx_pool: u64::from(tx_pool),
            ..Default::default()
        }
    }

    /// Folds a round-trip time sample into [`Self::srtt`].
    ///
    /// The samples include our own queue and the peer's queue, and the delay of the peer's muxer.
    /// That is intended, see `TRANSIT_CONTROL.md`.
    pub fn observe_rtt(&mut self, rtt: Duration) {
        let srtt = match self.srtt {
            None => rtt,
            Some(srtt) => {
                let srtt = srtt.as_secs_f64();
                Duration::from_secs_f64(srtt + (rtt.as_secs_f64() - srtt) * SRTT_GAIN)
            },
        };
        self.srtt = Some(srtt.max(SRTT_MIN));
    }

    /// Returns `srtt` in seconds: the time constant of the rate estimates, about one loop.
    ///
    /// [`None`] before the first round-trip time sample.
    pub fn loop_secs(&self) -> Option<f64> {
        self.srtt.map(|srtt| srtt.as_secs_f64())
    }

    /// Returns `tau`: the time span an allowance covers, in seconds, `k` loops.
    ///
    /// [`None`] before the first round-trip time sample.
    pub fn tau(&self) -> Option<f64> {
        self.loop_secs().map(|srtt| ALLOWANCE_RTTS * srtt)
    }

    /// Returns how much of the peer's pool is not in use.
    pub fn pool_room(&self) -> u64 {
        self.tx_pool.saturating_sub(self.tx_used)
    }

    /// Returns whether there is room in the peer's pool.
    pub fn room_left(&self) -> bool {
        self.pool_room() > 0
    }

    /// Returns the number of streams waiting for their turn.
    pub fn turns_len(&self) -> usize {
        self.turns.len()
    }

    /// Returns whether the stream may use shared room now.
    ///
    /// Every stream may, there is no queue yet.
    pub fn has_turn(&self, _me: &StreamRef) -> bool {
        true
    }

    /// Puts the stream at the back of the queue. The stream must not be in it.
    pub fn join_turns(&mut self, _me: &StreamRef) {}

    /// Removes the stream from the queue, wherever it is. Does nothing if it is not in it.
    pub fn leave_turns(&mut self, _me: &StreamRef) {}

    /// Wakes the stream that is first in line.
    ///
    /// The caller may hold the lock of one stream, `me`. It must pass that stream along with its poller,
    /// because the stream may be the one to wake and its lock cannot be taken again.
    pub fn wake_front(&self, _me: Option<(&StreamRef, &mut WakerSlot)>) {}
}
