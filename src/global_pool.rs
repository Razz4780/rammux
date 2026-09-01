//! State shared by all streams of a single rammux connection.

use std::time::Duration;

/// What every stream of a connection needs to see, and no more.
///
/// This is the polling strategy of the connection's task selector: it is
/// borrowed mutably while each stream decides what to put on the wire.
/// Everything session-level - the transit window, the `PING` machinery -
/// lives in the [`Transport`](crate::transport::Transport) instead; the
/// two fields the streams need from it are mirrored here once per pass,
/// which is also what keeps the selector from having to borrow it.
#[derive(Default)]
pub struct GlobalPool {
    /// Bytes still available in the shared receive window pool.
    ///
    /// Streams that sustain throughput borrow from it to size their own
    /// receive windows, and return what they borrowed when they close.
    pub available: usize,
    /// Latest loaded RTT, mirrored from the transport.
    ///
    /// Stream receive windows size against it rather than the clean
    /// sample, because a stream's credit loop runs through exactly the
    /// queues the loaded measurement includes.
    pub dirty_rtt: Option<Duration>,
    /// Most `DATA` payload the transport will accept right now, mirrored
    /// from [`Transport::send_budget`](crate::transport::Transport::send_budget).
    /// [`None`] means the transit window is disabled and payload is
    /// capped only by the stream's own window and the frame limit.
    pub send_budget: Option<u32>,
    /// Set by a stream whose outbound poll was held back solely by an
    /// exhausted transit window.
    ///
    /// Sticky: streams that returned pending on transit credit are not
    /// polled again until a grant wakes them, so the flag has to survive
    /// the passes in between. The connection clears it when credit
    /// arrives and when payload is actually emitted.
    pub transit_blocked: bool,
}
