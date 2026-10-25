use std::time::Duration;

/// Global pool of receive window capacity available to all rammux streams within a single rammux connection.
#[derive(Default)]
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
    /// Bytes of `DATA` written to the transport. Wraps.
    pub tx_bytes: u32,
    /// Bytes of `DATA` read from the transport, including discarded ones. Wraps.
    pub rx_bytes: u32,
}
