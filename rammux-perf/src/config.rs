use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use schemars::JsonSchema;
use serde::Deserialize;

/// Echo server configuration.
#[derive(Deserialize, JsonSchema)]
pub struct ServerConfig {
    /// Address of the server's HTTP API.
    ///
    /// The server talks only HTTP/1.1 and serves one endpoint - `/echo`.
    /// The endpoint can be used to upgrade the connection to one of the following multiplexing protocols:
    /// `rammux`, `h2`, `yamux`. Configuration of the server's multiplexer is passed in the UPGRADE request headers.
    pub http_addr: SocketAddr,

    /// Address of the server's HTTPS API.
    ///
    /// The server talks only HTTP/1.1 and serves one endpoint - `/echo`.
    /// The endpoint can be used to upgrade the connection to one of the following multiplexing protocols:
    /// `rammux`, `h2`, `yamux`. Configuration of the server's multiplexer is passed in the UPGRADE request headers.
    pub https_addr: SocketAddr,

    /// Path to a PEM file with a TLS certificate and key to be used by the server.
    pub cert_path: PathBuf,
}

/// Client configuration.
///
/// The client runs a number of iterations against the echo server. Each
/// iteration opens a fresh connection and runs some number of bulk streams and
/// at most one ping pong stream on it:
///
/// * A bulk stream sends data as fast as possible and reads the echo back. These
///   streams measure throughput.
/// * The ping pong stream sends one message and waits for its echo before sending
///   the next. It measures latency under whatever load the bulk streams put on
///   the connection.
///
/// After each iteration the client logs:
/// * CPU time spent on the iteration, user and sys, in ms
/// * mean/p50/p99 throughput across the bulk streams, in Mbit/s - bytes a stream
///   sent per second, with the time it took the echo to come back included
/// * mean/p50/p99 latency across the ping pong exchanges, in ms, and how many
///   exchanges completed
/// * total elapsed time, connection setup included
///
/// The iteration ends when every bulk stream has read its echo back. An
/// outstanding ping pong message is abandoned. A failed iteration is logged and
/// retried, up to 3 times.
#[derive(Deserialize, JsonSchema)]
pub struct ClientConfig {
    /// Address of the echo server's HTTP API - or of its HTTPS API, when `cert_path` is set.
    pub server_addr: SocketAddr,

    /// Path to a PEM file with the certificate the server presents.
    ///
    /// When set, the connection is encrypted with TLS 1.3, and this certificate
    /// is the only trust root.
    #[serde(default)]
    pub cert_path: Option<PathBuf>,

    /// Number of iterations to run.
    #[serde(default = "default_iterations")]
    pub iterations: NonZeroUsize,

    /// How many bulk streams to run in each iteration.
    pub bulk_streams: NonZeroUsize,

    /// How much data each bulk stream sends, and reads back, in bytes.
    #[serde(default = "default_bulk_stream_data")]
    pub bulk_stream_data: NonZeroUsize,

    /// Size of the ping pong message, in bytes.
    ///
    /// If not set, the iteration runs no ping pong stream.
    #[serde(default)]
    pub ping_pong_size: Option<NonZeroUsize>,

    /// The multiplexer to run, and its configuration.
    ///
    /// The configuration is also sent to the server in the upgrade request, so
    /// both sides run the protocol with matching settings.
    pub muxer: MuxerConfig,
}

fn default_iterations() -> NonZeroUsize {
    NonZeroUsize::MIN
}

fn default_bulk_stream_data() -> NonZeroUsize {
    NonZeroUsize::new(1024 * 1024).unwrap()
}

impl ClientConfig {
    /// How many streams an iteration opens.
    pub fn streams(&self) -> usize {
        self.bulk_streams.get() + usize::from(self.ping_pong_size.is_some())
    }
}

/// Which multiplexer to run, and how to configure it.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "protocol", rename_all = "lowercase")]
pub enum MuxerConfig {
    /// rammux.
    Rammux(RammuxMuxerConfig),
    /// yamux.
    Yamux(YamuxMuxerConfig),
    /// HTTP/2, one request per stream.
    H2(H2MuxerConfig),
}

impl MuxerConfig {
    /// Name of the protocol, as used in the `Upgrade` header.
    pub fn protocol(&self) -> &'static str {
        match self {
            Self::Rammux(..) => "rammux",
            Self::Yamux(..) => "yamux",
            Self::H2(..) => "h2",
        }
    }
}

/// rammux configuration, from the client's point of view.
///
/// Settings a peer has to agree on with the other side are sent to the server
/// mirrored (its `remote` is our `local`); the rest are sent as they are, and
/// the server uses the same values.
#[derive(Deserialize, JsonSchema)]
pub struct RammuxMuxerConfig {
    /// Limit for the length of data in a single `DATA` frame, in bytes.
    pub frame_limit: NonZeroU32,
    /// Initial receive window of every stream on this side, in bytes.
    pub local_recv_window: NonZeroU32,
    /// Initial receive window of every stream on the server's side, in bytes.
    pub remote_recv_window: NonZeroU32,
    /// Global receive window pool that streams borrow from, in bytes.
    pub global_recv_window: usize,
    /// Initial transit window this side grants the server, in bytes. `0` disables it.
    pub local_transit_window: u32,
    /// Initial transit window the server grants this side, in bytes. `0` disables it.
    pub remote_transit_window: u32,
    /// Autotune limit of the transit window, in bytes.
    pub transit_window_max: u32,
    /// Interval of the link-clearing probe, in seconds. Also its timeout.
    pub probe_interval: NonZeroU64,
    /// Interval of the plain ping, in seconds. Also its timeout.
    pub ping_interval: NonZeroU64,
}

/// yamux configuration. Both sides use the same values.
#[derive(Deserialize, JsonSchema)]
pub struct YamuxMuxerConfig {
    /// Largest data frame either side sends, in bytes.
    pub frame_limit: usize,
    /// Maximum connection-level receive window, in bytes.
    pub max_conn_receive_window: usize,
}

/// HTTP/2 configuration. Both sides use the same values.
#[derive(Deserialize, JsonSchema)]
pub struct H2MuxerConfig {
    /// Whether to size flow control windows from bandwidth-delay estimates.
    pub adaptive_window: bool,
    /// Largest frame either side sends, in bytes.
    pub frame_limit: u32,
    /// Send buffer size per stream, in bytes.
    pub max_send_buf_size: usize,
    /// Initial stream-level flow control window, in bytes.
    pub initial_stream_window: u32,
    /// Initial connection-level flow control window, in bytes.
    pub initial_connection_window: u32,
}
