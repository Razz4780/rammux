use std::time::Duration;

/// Global pool of receive window capacity available to all rammux streams within a single rammux connection.
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
}
