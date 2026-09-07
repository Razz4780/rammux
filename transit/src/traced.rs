//! Reading a connection's window state back out for the trace log.
//!
//! Both ends need it. Each end's window governs the direction it *receives*
//! on, so a client's log describes the echo coming back and says nothing about
//! the window that is actually limiting the client's own sending. Diagnosing
//! either one means logging both.

use tokio::net::TcpStream;

use transit::{Stats, Transit};

use crate::sockopt;

pub trait Traced {
    fn stats(&self) -> Option<Stats>;
    /// The socket underneath. Its buffers are read back at the end of a run,
    /// not at connect: autotuning grows them while the run is happening, and
    /// the value that mattered is the one it settled on.
    fn socket(&self) -> &TcpStream;
}

impl Traced for Transit<TcpStream> {
    fn stats(&self) -> Option<Stats> {
        Some(Transit::stats(self))
    }

    fn socket(&self) -> &TcpStream {
        self.get_ref()
    }
}

impl Traced for TcpStream {
    fn stats(&self) -> Option<Stats> {
        None
    }

    fn socket(&self) -> &TcpStream {
        self
    }
}

/// One trace line, in the same shape from either end.
pub fn line(io: &impl Traced, at: f64, echo_ms: f64) -> String {
    let stats = io.stats();
    let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
    format!(
        "TRACE t={at:.3} window={} credit={} rate_mbps={:.1} clean_rtt_ms={:.1} \
         echo_ms={echo_ms:.1} stalls={} queue_ms={:.1} desired={}",
        stats.map_or(0, |stats| stats.window),
        stats.map_or(0, |stats| stats.credit),
        stats.map_or(0.0, |stats| stats.rate * 8.0 / 1e6),
        stats.and_then(|stats| stats.clean_rtt).map_or(0.0, ms),
        stats.map_or(0, |stats| stats.stalls),
        stats.and_then(|stats| stats.queued).map_or(-1.0, ms),
        stats.map_or(0, |stats| stats.desired),
    )
}

/// Everything the socket settled on, for the end of a run.
pub fn socket_info(io: &impl Traced) -> sockopt::SocketInfo {
    sockopt::describe(io.socket())
}
