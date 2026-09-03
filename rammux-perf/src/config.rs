use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use hyper::header::HeaderName;
use rammux::config::RammuxConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    #[serde(default = "non_zero_min")]
    pub iterations: NonZeroUsize,

    /// How many bulk streams to run in each iteration.
    #[serde(default = "non_zero_min")]
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

fn non_zero_min() -> NonZeroUsize {
    NonZeroUsize::MIN
}

fn default_bulk_stream_data() -> NonZeroUsize {
    NonZeroUsize::new(1024 * 1024).unwrap()
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
    /// Name of the header that carries JSON-serialized inner config.
    pub const HEADER_NAME: HeaderName = HeaderName::from_static("muxer-config");

    /// Name of the protocol, as used in the `Upgrade` header.
    pub fn protocol(&self) -> &'static str {
        match self {
            Self::Rammux(..) => "rammux",
            Self::Yamux(..) => "yamux",
            Self::H2(..) => "h2",
        }
    }
}

/// rammux configuration, from the client's point of view. Both sides use the same values.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct RammuxMuxerConfig {
    /// Size limit for a single data frame payload, in bytes.
    pub frame_limit: NonZeroU32,
    /// Initial receive window for a stream, in bytes.
    pub stream_recv_window: NonZeroU32,
    /// Global receive window pool that streams borrow from, in bytes.
    pub global_recv_window: usize,
    /// Initial transit window, in bytes. `0` disables it.
    pub transit_window: u32,
    /// Autotune limit of the transit window, in bytes.
    pub transit_window_max: u32,
    /// Interval of the link-clearing probe, in seconds. Also its timeout.
    pub probe_interval: NonZeroU64,
    /// Interval of the plain ping, in seconds. Also its timeout.
    pub ping_interval: NonZeroU64,
    /// Limit for the number of concurrent streams. Applies separately to inbound and outbound streams.
    pub max_streams: u32,
}

impl RammuxMuxerConfig {
    /// The [`RammuxConfig`] both sides run with.
    ///
    /// One place for this, because it is easy to get one of the mirrored
    /// windows wrong in one of two copies and impossible to notice until the
    /// peer rejects a frame.
    pub fn to_rammux_config(&self) -> RammuxConfig {
        let mut config = RammuxConfig::new();
        config.frame_limit = self.frame_limit;
        config.local_recv_window = self.stream_recv_window;
        config.remote_recv_window = self.stream_recv_window.get();
        config.global_recv_window = self.global_recv_window;
        config.local_transit_window = self.transit_window;
        config.remote_transit_window = self.transit_window;
        config.transit_window_max = self.transit_window_max;
        config.max_inbound_streams = self.max_streams;
        config.max_outbound_streams = self.max_streams;
        config
    }

    /// The probe schedule both sides run with.
    pub fn probe_interval(&self) -> Duration {
        Duration::from_secs(self.probe_interval.get())
    }

    /// The plain ping schedule both sides run with.
    pub fn ping_interval(&self) -> Duration {
        Duration::from_secs(self.ping_interval.get())
    }
}

/// yamux configuration. Both sides use the same values.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct YamuxMuxerConfig {
    /// Size limit for a single data frame payload, in bytes.
    pub split_send_size: usize,
    /// Limit for the total receive window across all streams, in bytes.
    ///
    /// Must be `>= 256 * 1024 * max_num_streams`.
    pub max_connection_receive_window: usize,
    /// Limit for the number of concurrent streams.
    pub max_num_streams: usize,
}

impl YamuxMuxerConfig {
    /// The [`yamux::Config`] both sides run with.
    ///
    /// yamux asserts its window constraint inside each setter, against
    /// whatever the other setting is at that moment - so the check happens
    /// here first, as an error rather than a panic in a connection task, and
    /// the setters run in an order that cannot trip it for a valid config.
    pub fn to_yamux_config(&self) -> anyhow::Result<yamux::Config> {
        const DEFAULT_STREAM_WINDOW: usize = 256 * 1024;
        anyhow::ensure!(
            self.max_connection_receive_window >= self.max_num_streams * DEFAULT_STREAM_WINDOW,
            "max_connection_receive_window ({}) must be at least 256 KiB * max_num_streams ({})",
            self.max_connection_receive_window,
            self.max_num_streams,
        );
        let mut config = yamux::Config::default();
        config
            .set_max_num_streams(self.max_num_streams)
            .set_max_connection_receive_window(Some(self.max_connection_receive_window))
            .set_split_send_size(self.split_send_size)
            .set_read_after_close(true);
        Ok(config)
    }
}

/// HTTP/2 configuration. Both sides use the same values.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct H2MuxerConfig {
    /// Enable adaptive flow control.
    ///
    /// Adaptive flow control overrides `initial_connection_window_size` and `initial_stream_window_size`.
    pub adaptive_window: bool,
    /// Frame size limit.
    pub max_frame_size: u32,
    /// Limit for the number of concurrent streams.
    pub max_concurrent_streams: u32,
    /// Limit for the write buffer size for each stream.
    pub max_send_buf_size: usize,
    /// Initial connection receive window, in bytes.
    pub initial_connection_window_size: u32,
    /// Initial stream receive window, in bytes.
    pub initial_stream_window_size: u32,
}
