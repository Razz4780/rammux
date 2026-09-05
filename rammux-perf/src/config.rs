use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Context as _;
use hyper::header::HeaderName;
use rammux::config::{RammuxConfig, TransitGrowth};
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

    /// Address of the server's QUIC API.
    ///
    /// QUIC is UDP, and its TLS handshake is the connection's own, so there is
    /// no HTTP upgrade here and no plaintext variant - a QUIC run is always
    /// encrypted, and compares against the other protocols' HTTPS addresses.
    pub quic_addr: SocketAddr,

    /// Path to a PEM file with a TLS certificate and key to be used by the server.
    pub cert_path: PathBuf,

    /// QUIC settings the server runs with.
    ///
    /// Defaulted, so a config for a server that will never see a QUIC client
    /// need not spell it out.
    ///
    /// The odd one out: every other protocol takes its settings from the
    /// client's upgrade request, so the two sides cannot disagree. QUIC's
    /// windows and stream limits are transport parameters, fixed in the
    /// handshake, so the server has to know them before a client says
    /// anything. The client sends its copy on a control stream anyway and the
    /// server refuses a connection whose config differs from this one, which
    /// buys back the guarantee the header gave the others.
    #[serde(default)]
    pub quic: QuicMuxerConfig,
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

    /// `host:port` to wait for before the first iteration.
    ///
    /// The client blocks until this accepts a TCP connection. A cluster run
    /// uses it as a start gate: the client has to be running for its link to
    /// be impaired, but must not connect until it is, and this holds it in
    /// between. Unset starts immediately.
    #[serde(default)]
    pub await_endpoint: Option<String>,
}

fn default_transit_update_threshold() -> NonZeroU32 {
    NonZeroU32::new(64 * 1024).unwrap()
}

fn non_zero_min() -> NonZeroUsize {
    NonZeroUsize::MIN
}

fn default_bulk_stream_data() -> NonZeroUsize {
    NonZeroUsize::new(1024 * 1024).unwrap()
}

/// Which multiplexer to run, and how to configure it.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(tag = "protocol", rename_all = "lowercase")]
pub enum MuxerConfig {
    /// rammux.
    Rammux(RammuxMuxerConfig),
    /// yamux.
    Yamux(YamuxMuxerConfig),
    /// HTTP/2, one request per stream.
    H2(H2MuxerConfig),
    /// QUIC, one bidirectional stream per logical stream.
    Quic(QuicMuxerConfig),
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
            Self::Quic(..) => "quic",
        }
    }
}

/// rammux configuration, from the client's point of view. Both sides use the same values.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct RammuxMuxerConfig {
    /// Initial receive window for a stream, in bytes.
    pub stream_recv_window: NonZeroU32,
    /// Global receive window pool that streams borrow from, in bytes.
    pub global_recv_window: usize,
    /// Initial transit window, in bytes. `0` disables it.
    pub transit_window: u32,
    /// Autotune limit of the transit window, in bytes.
    pub transit_window_max: u32,
    /// How the transit window grows towards the size of the path.
    #[serde(default)]
    pub transit_growth: TransitGrowthConfig,
    /// How much freed transit credit is re-granted at once, in bytes, or half
    /// the window if that is smaller. `4294967295` reproduces the old
    /// half-window behaviour on any window.
    #[serde(default = "default_transit_update_threshold")]
    pub transit_update_threshold: NonZeroU32,
    /// Interval of the link-clearing probe, in seconds. Also its timeout.
    pub probe_interval: NonZeroU64,
    /// Interval of the plain ping, in seconds. Also its timeout.
    pub ping_interval: NonZeroU64,
}

impl RammuxMuxerConfig {
    /// The [`RammuxConfig`] both sides run with.
    ///
    /// One place for this, because it is easy to get one of the mirrored
    /// windows wrong in one of two copies and impossible to notice until the
    /// peer rejects a frame.
    pub fn to_rammux_config(&self) -> RammuxConfig {
        let mut config = RammuxConfig::new();
        config.frame_limit = NonZeroU32::new(16 * 1024).unwrap();
        config.local_recv_window = self.stream_recv_window;
        config.remote_recv_window = self.stream_recv_window.get();
        config.global_recv_window = self.global_recv_window;
        config.local_transit_window = self.transit_window;
        config.remote_transit_window = self.transit_window;
        config.transit_window_max = self.transit_window_max;
        config.transit_update_threshold = self.transit_update_threshold;
        config.transit_growth = match self.transit_growth {
            TransitGrowthConfig::RateCeiling => TransitGrowth::RateCeiling,
            TransitGrowthConfig::RatePlateau => TransitGrowth::RatePlateau,
        };
        config.max_inbound_streams = 100;
        config.max_outbound_streams = 100;
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

/// How rammux's transit window grows. Mirrors [`TransitGrowth`], which the
/// library does not serialize.
#[derive(Deserialize, Serialize, JsonSchema, PartialEq, Eq, Debug, Default, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum TransitGrowthConfig {
    /// Doubles while window-limited, ceilinged at 2 x clean RTT x the
    /// measured arrival rate - which the window itself limits, so in
    /// practice the window crawls.
    #[default]
    RateCeiling,
    /// Doubles while each doubling still raises the inbound rate by a
    /// quarter; direction-correct and needs no ping.
    RatePlateau,
}

/// yamux configuration. Both sides use the same values.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct YamuxMuxerConfig {
    /// Limit for the total receive window across all streams, in bytes.
    ///
    /// Must be `>= 256 * 1024 * max_num_streams`.
    ///
    /// Every stream initially has a 256kb window, and the window is autotuned.
    /// This value sets an upper limit for the total size of all windows.
    pub global_recv_window: usize,
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
            self.global_recv_window >= 100 * DEFAULT_STREAM_WINDOW,
            "global_recv_window ({}) must be at least 256 KiB * 100",
            self.global_recv_window,
        );
        let mut config = yamux::Config::default();
        config
            .set_max_num_streams(100)
            .set_max_connection_receive_window(Some(self.global_recv_window))
            .set_split_send_size(16 * 1024)
            .set_read_after_close(true);
        Ok(config)
    }
}

/// QUIC configuration. Both sides use the same values.
///
/// Compare with [`H2MuxerConfig`]: the same three knobs, so the two protocols
/// can be given the same budget and the difference measured is the protocol
/// rather than the sizing.
#[derive(Deserialize, Serialize, JsonSchema, PartialEq, Eq, Debug)]
#[serde(default)]
pub struct QuicMuxerConfig {
    /// Fixed size of each stream's receive window.
    pub stream_recv_window: u32,
    /// Upper limit for the total size of all streams' receive windows.
    pub global_recv_window: u32,
    /// Limit for the number of concurrent streams.
    pub max_streams: u32,
    /// Which congestion controller to run.
    ///
    /// All of a QUIC connection's streams share one, so this is the single
    /// thing that decides how the whole connection behaves on a lossy path -
    /// unlike the windows, which the measurements show are never the limit.
    pub congestion: CongestionControl,
    /// `SO_RCVBUF` for the UDP socket, in bytes.
    ///
    /// A QUIC receiver that cannot drain its socket fast enough drops
    /// datagrams there, and its peer's congestion controller reads those
    /// drops as congestion. The default is whatever `net.core.rmem_default`
    /// says, which is 208 KiB on a stock kernel - under a millisecond of
    /// buffering at these rates. `net.core.rmem_max` caps what the kernel
    /// will grant.
    pub socket_recv_buffer: Option<usize>,
    /// `SO_SNDBUF` for the UDP socket, in bytes.
    pub socket_send_buffer: Option<usize>,
    /// Datagram size to start at, before path MTU discovery raises it.
    ///
    /// QUIC's floor is 1200, which is what quinn starts from; a path known to
    /// carry more need not spend the discovery on finding that out.
    pub initial_mtu: Option<u16>,
}

impl QuicMuxerConfig {
    /// A UDP socket for a QUIC endpoint, with the buffers this config asks
    /// for.
    ///
    /// quinn binds its own socket unless it is handed one, and the default
    /// receive buffer is small enough that a fast sender overruns it. Those
    /// drops are indistinguishable from congestion to the peer, so they cost
    /// throughput out of all proportion to the memory saved.
    ///
    /// The kernel silently halves what it grants (`SO_RCVBUF` is reported
    /// doubled) and clamps to `net.core.rmem_max`, so the size asked for is
    /// an upper bound rather than a promise. Both are logged.
    pub fn bind_socket(&self, addr: SocketAddr) -> anyhow::Result<std::net::UdpSocket> {
        let domain = if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
                .context("failed to create a UDP socket")?;
        if let Some(size) = self.socket_recv_buffer {
            socket
                .set_recv_buffer_size(size)
                .with_context(|| format!("failed to set SO_RCVBUF to {size}"))?;
        }
        if let Some(size) = self.socket_send_buffer {
            socket
                .set_send_buffer_size(size)
                .with_context(|| format!("failed to set SO_SNDBUF to {size}"))?;
        }
        socket
            .bind(&addr.into())
            .with_context(|| format!("failed to bind a UDP socket on {addr}"))?;
        tracing::info!(
            %addr,
            recv_buffer = socket.recv_buffer_size().ok(),
            send_buffer = socket.send_buffer_size().ok(),
            "Bound the QUIC socket",
        );
        Ok(socket.into())
    }
}

/// The congestion controller a QUIC connection runs.
#[derive(Deserialize, Serialize, JsonSchema, PartialEq, Eq, Debug, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum CongestionControl {
    /// quinn's default. Loss-based: every loss is read as congestion, so a
    /// path that drops packets for any other reason costs throughput.
    #[default]
    Cubic,
    /// Rate-based: models the bottleneck bandwidth and round trip rather than
    /// counting losses, so random loss costs it much less.
    Bbr,
    /// The reference loss-based controller. Slower to recover than Cubic.
    NewReno,
}

impl Default for QuicMuxerConfig {
    /// What `quinn` itself defaults to, near enough: a 1 MiB stream window,
    /// 8 MiB across the connection, and 100 streams. Only reached by a server
    /// whose config says nothing about QUIC, which is every server that is
    /// not benchmarking it.
    fn default() -> Self {
        Self {
            stream_recv_window: 1024 * 1024,
            global_recv_window: 8 * 1024 * 1024,
            max_streams: 100,
            congestion: CongestionControl::Cubic,
            socket_recv_buffer: None,
            socket_send_buffer: None,
            initial_mtu: None,
        }
    }
}

impl QuicMuxerConfig {
    /// The [`quinn::TransportConfig`] both sides run with.
    pub fn to_transport_config(&self) -> quinn::TransportConfig {
        let mut config = quinn::TransportConfig::default();
        config.congestion_controller_factory(match self.congestion {
            CongestionControl::Cubic => {
                Arc::new(quinn::congestion::CubicConfig::default()) as Arc<_>
            },
            CongestionControl::Bbr => Arc::new(quinn::congestion::BbrConfig::default()) as Arc<_>,
            CongestionControl::NewReno => {
                Arc::new(quinn::congestion::NewRenoConfig::default()) as Arc<_>
            },
        });
        if let Some(mtu) = self.initial_mtu {
            config.initial_mtu(mtu);
        }
        config
            .stream_receive_window(self.stream_recv_window.into())
            .receive_window(self.global_recv_window.into())
            .max_concurrent_bidi_streams(self.max_streams.into())
            // The workload opens one uni stream, to carry the config. Nothing
            // else uses them, and leaving the default in place would let a
            // peer open streams this benchmark would never read.
            .max_concurrent_uni_streams(1u32.into())
            // Nothing here is idle: a connection that goes quiet has stalled,
            // and should fail rather than be timed out and counted as a slow
            // iteration.
            .keep_alive_interval(Some(Duration::from_secs(5)));
        config
    }
}

/// HTTP/2 configuration. Both sides use the same values.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct H2MuxerConfig {
    /// Enable adaptive flow control.
    ///
    /// Adaptive flow control overrides `stream_recv_window` and `gloval_recv_window`.
    pub adaptive_window: bool,
    /// Fixed size of each stream's receive window.
    ///
    /// Ignored if `adaptive_window` is set.
    pub stream_recv_window: u32,
    /// Upper limit for the total size of all streams' receive windows.
    ///
    /// Ignored if `adaptive_window` is set.
    pub global_recv_window: u32,
}
