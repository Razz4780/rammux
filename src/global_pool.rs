use std::{collections::VecDeque, sync::Arc, time::Duration};

use tokio::time::Instant;

use crate::{
    delay::{DelayControl, PongSample},
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
    /// The connection window, with `TransitControl::CopaLike`.
    pub dc: Option<DelayControl>,
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

    /// Turns on the connection window of `TransitControl::CopaLike`.
    pub fn with_delay_control(mut self, dc: DelayControl) -> Self {
        self.dc = Some(dc);
        self
    }

    /// Takes a `PONG` into account: the round trip time, and the window.
    pub fn on_pong(&mut self, pong: PongSample, rtt: Duration) {
        self.observe_rtt(rtt);
        if let (Some(dc), Some(srtt)) = (&mut self.dc, self.srtt) {
            dc.on_pong(pong, srtt, self.tx_bytes);
        }
    }

    /// The room in the connection window, [`None`] if there is no window.
    ///
    /// It includes the pacer: what the window allows and the pacer has released.
    pub fn window_room(&self, now: Instant) -> Option<u64> {
        self.dc
            .as_ref()
            .map(|dc| dc.room(now, self.tx_bytes).min(dc.pace_room(now)))
    }

    /// Tells the window or the pacer that it kept `wanted` bytes from being sent.
    /// Only the window counts as limiting for the growth of `W`.
    pub fn note_window_limit(&mut self, now: Instant, wanted: u64) {
        let tx_bytes = self.tx_bytes;
        if let Some(dc) = &mut self.dc {
            if dc.room(now, tx_bytes) < wanted {
                dc.note_limited(now);
            } else if dc.pace_room(now) < wanted {
                dc.note_paced(now, wanted);
            }
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

    /// Returns whether there is room in the peer's pool and in the connection window.
    pub fn room_left(&self, now: Instant) -> bool {
        self.pool_room() > 0 && self.window_room(now).is_none_or(|room| room > 0)
    }

    /// Returns the number of streams waiting for their turn.
    pub fn turns_len(&self) -> usize {
        self.turns.len()
    }

    /// Returns the streams waiting for their turn, first in line first.
    #[cfg(test)]
    pub fn turn_order(&self) -> Vec<StreamRef> {
        self.turns.iter().cloned().collect()
    }

    /// Returns whether the stream may use shared room now:
    /// nobody waits, or the stream is first in line.
    pub fn has_turn(&self, me: &StreamRef) -> bool {
        self.turns
            .front()
            .is_none_or(|front| Arc::ptr_eq(front, me))
    }

    /// Puts the stream at the back of the queue. The stream must not be in it.
    pub fn join_turns(&mut self, me: &StreamRef) {
        self.turns.push_back(me.clone());
    }

    /// Puts the stream at the front of the queue. The stream must not be in it.
    pub fn join_turns_front(&mut self, me: &StreamRef) {
        self.turns.push_front(me.clone());
    }

    /// Removes the stream from the queue, wherever it is. Does nothing if it is not in it.
    pub fn leave_turns(&mut self, me: &StreamRef) {
        if self
            .turns
            .front()
            .is_some_and(|front| Arc::ptr_eq(front, me))
        {
            self.turns.pop_front();
        } else {
            self.turns.retain(|queued| !Arc::ptr_eq(queued, me));
        }
    }

    /// Wakes the stream that is first in line.
    ///
    /// The caller may hold the lock of one stream, `me`. It must pass that stream along with its poller,
    /// because the stream may be the one to wake and its lock cannot be taken again.
    pub fn wake_front(&self, me: Option<(&StreamRef, &mut WakerSlot)>) {
        let Some(front) = self.turns.front() else {
            return;
        };
        match me {
            Some((me, poller)) if Arc::ptr_eq(front, me) => poller.wake(),
            _ => front.lock().unwrap().updates_poller.wake(),
        }
    }
}
