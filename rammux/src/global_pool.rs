//! State shared by all streams of a single rammux connection.

use std::time::Duration;

use crate::ping::Ping;

/// Global state shared by all rammux streams within a single rammux connection.
///
/// Used as the polling strategy of the connection's task selector.
pub struct GlobalPool {
    /// Latest loaded round trip of the connection, from the plain ping: the
    /// round trip as it actually is with the connection's queues standing -
    /// the transit layer's credit wait, the socket buffers and the path.
    /// Stream receive windows size from it, because a stream's credit loop
    /// runs through those same queues.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
    /// The connection's `PING` exchanges.
    pub ping: Ping,
}

impl Default for GlobalPool {
    fn default() -> Self {
        Self {
            rtt: None,
            available: 0,
            ping: Ping::new(),
        }
    }
}
