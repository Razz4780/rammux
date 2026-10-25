use std::time::Duration;

use crate::transit::{SRTT_GAIN, SRTT_MIN};

/// Connection-level state shared by all streams of a rammux connection.
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
}

impl GlobalPool {
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

    /// Creates the state of a new connection.
    pub fn new(rx_pool: u32) -> Self {
        Self {
            rx_pool: u64::from(rx_pool),
            ..Default::default()
        }
    }
}
