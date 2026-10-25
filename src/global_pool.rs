use std::time::Duration;

/// Connection-level state shared by all streams of a rammux connection.
#[derive(Default)]
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    pub rtt: Option<Duration>,
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
    /// Creates the state of a new connection.
    pub fn new(rx_pool: u32) -> Self {
        Self {
            rx_pool: u64::from(rx_pool),
            ..Default::default()
        }
    }
}
