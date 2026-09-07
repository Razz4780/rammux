//! Socket settings a result is unattributable without.
//!
//! `SO_SNDBUF` is a second limiter sitting behind the transit window: smaller
//! than the window, it binds instead and sets the latency floor, and the number
//! that comes out is then a measurement of the socket buffer rather than of the
//! protocol. The congestion control matters for the same reason - cubic and bbr
//! are not comparable - and it has been known to change underneath a session.

use std::{fmt, io};

use socket2::SockRef;
use tokio::net::TcpStream;

/// What the socket was actually configured with, as opposed to asked for.
pub struct SocketInfo {
    /// As reported by the kernel, which doubles what was requested to cover
    /// its own bookkeeping.
    pub send_buffer: usize,
    pub recv_buffer: usize,
    pub congestion_control: String,
}

impl fmt::Display for SocketInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sndbuf={} rcvbuf={} cc={}",
            self.send_buffer, self.recv_buffer, self.congestion_control
        )
    }
}

/// Applies the settings the protocol depends on, and reports what stuck.
///
/// `send_buffer` pins `SO_SNDBUF`, which also turns the kernel's autotuning
/// off. Left unset, autotuning runs and the ceiling is `net.ipv4.tcp_wmem`,
/// which the harness raises out of the way.
pub fn prepare(conn: &TcpStream, send_buffer: Option<usize>) -> io::Result<SocketInfo> {
    // Credit returns are 8 byte frames on the latency path, and Nagle would
    // hold every one of them behind the data already in flight.
    conn.set_nodelay(true)?;

    if let Some(bytes) = send_buffer {
        SockRef::from(conn).set_send_buffer_size(bytes)?;
    }
    Ok(describe(conn))
}

/// Reads back what the socket is currently configured with.
///
/// Worth calling at the end of a run as well as the start: with autotuning on,
/// the buffer that mattered is the one it grew to, not the one it opened with.
pub fn describe(conn: &TcpStream) -> SocketInfo {
    let socket = SockRef::from(conn);
    SocketInfo {
        send_buffer: socket.send_buffer_size().unwrap_or(0),
        recv_buffer: socket.recv_buffer_size().unwrap_or(0),
        // The per-namespace setting, which is the one that applies.
        congestion_control: std::fs::read_to_string("/proc/sys/net/ipv4/tcp_congestion_control")
            .map_or_else(|_| "unknown".to_string(), |cc| cc.trim().to_string()),
    }
}
