//! rammux connection configuration.

use std::{fmt, num::NonZeroU32, time::Duration};

/// Role in a rammux connection.
///
/// The only difference between the roles in a rammux connection
/// is the pool of [`StreamId`](crate::StreamId)s
/// that can be used when starting a new stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RammuxRole {
    /// Can initiate streams with even IDs.
    Client,
    /// Can initiate streams with odd IDs.
    Server,
}

impl fmt::Display for RammuxRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client => f.write_str("client"),
            Self::Server => f.write_str("server"),
        }
    }
}

/// Configuration for a [`RammuxConnection`](crate::connection::RammuxConnection).
///
/// rammux does not define an in-band handshake for transport parameters.
/// Before running a rammux connection, the application must ensure that both sides use a compatible config.
/// Settings that have to be negotiated beforehand:
/// - [`RammuxConfig::frame_limit`]
/// - [`RammuxConfig::max_outbound_streams`]
/// - [`RammuxConfig::max_inbound_streams`]
/// - [`RammuxConfig::remote_recv_window`]
/// - [`RammuxConfig::local_recv_window`]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RammuxConfig {
    /// Limit for the length of data that can be sent in a single `DATA` frame.
    ///
    /// # Negotiation
    ///
    /// This limit applies to both sides of the connection, has to be negotiated beforehand,
    /// and must match on both sides.
    pub frame_limit: NonZeroU32,
    /// How many concurrent inbound (initiated by the remote side of the connection) streams we allow.
    ///
    /// # Negotiation
    ///
    /// This limit has to be negotiated beforehand and
    /// must not be smaller than the peer's [`RammuxConfig::max_outbound_streams`].
    pub max_inbound_streams: u32,
    /// How many concurrent outbound (initiated by the local side of the connection) streams we allow.
    ///
    /// # Negotiation
    ///
    /// This limit has to be negotiated beforehand and
    /// not exceed the peer's [`RammuxConfig::max_inbound_streams`].
    pub max_outbound_streams: u32,
    /// Initial size of the local receive window for every stream.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::remote_recv_window`].
    pub local_recv_window: NonZeroU32,
    /// Initial size of the remote receive window for every stream.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::local_recv_window`].
    pub remote_recv_window: u32,
    /// Interval on which `PING` frames will be sent to the peer.
    ///
    /// This interval also determines `PING` response timeout - if we don't receive
    /// the response before it's time to send the next `PING`, the connection fails.
    ///
    /// This value is a local know and does not have to be negotiated.
    pub ping_interval: Duration,
    /// Size of the global local receive window shared between all streams.
    ///
    /// This pool will be used for autotuning local receive windows of streams
    /// that are limited by flow control. Such streams will "borrow" window size from the pool,
    /// allowing the remote peer to spend less time waiting on window updates.
    ///
    /// This value is a local know and does not have to be negotiated.
    pub global_recv_window: usize,
    /// Initial size of the session-level transit window we grant to the peer.
    ///
    /// The transit window bounds the amount of `DATA` payload bytes that can be
    /// in flight between the peers. Credit is returned with `SESSION_WINDOW_UPDATE`
    /// frames as soon as data is received and stored in the muxer,
    /// independently of when the application reads it.
    ///
    /// `0` disables the transit window in this direction.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::remote_transit_window`].
    pub local_transit_window: u32,
    /// Initial size of the transit window granted to us by the peer.
    ///
    /// `0` means the peer does not limit our in-flight data.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::local_transit_window`].
    pub remote_transit_window: u32,
    /// Autotune limit for the local transit window.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub transit_window_max: u32,
    /// Use a sliding-window minimum of measured RTTs for transit window autotuning,
    /// instead of the latest sample.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub transit_min_rtt_filter: bool,
    /// Allow transit window growth only while the payload arrival rate
    /// exceeds the best rate observed so far.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub transit_bw_gate: bool,
    /// Size the transit window from RTT samples taken over a cooperatively
    /// cleared link (`CLEAR_LINK` probes), grow-only.
    ///
    /// Requires a peer that supports `CLEAR_LINK`; negotiate beforehand.
    pub transit_clean_probe: bool,
    /// Use the clean-probe RTT for every window-sizing decision: the
    /// per-stream window autotune and the transit rate meter, in addition
    /// to the transit growth policy. Without [`Self::transit_clean_probe`]
    /// the only clean sample is the connection's first ping.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub clean_rtt_sizing: bool,
    /// Per-stream receive window autotune gain: each stream's window
    /// targets `gain x consumption rate x RTT`.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub stream_window_gain: f64,
    /// Per-stream receive window growth limit: a window can at most
    /// multiply by this factor in a single update round. Must be >= 1.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub stream_window_growth: u32,
}

impl RammuxConfig {
    /// Creates a new config for the given [`RammuxRole`].
    ///
    /// Note that the obtained config will use default values for all settings.
    /// You will need to adjust settings that have to be negotiated with the peer.
    ///
    /// Default values:
    /// 1. [`Self::frame_limit`] - 16kb
    /// 2. [`Self::max_inbound_streams`] and [`Self::max_outbound_streams`] - 128
    /// 3. [`Self::local_recv_window`] and [`Self::remote_recv_window`] - 64kb
    /// 4. [`Self::ping_interval`] - 5s
    /// 5. [`Self::global_recv_window`] - 4mb
    /// 6. [`Self::local_transit_window`] and [`Self::remote_transit_window`] - 0 (disabled)
    /// 7. [`Self::transit_window_max`] - 4mb
    pub const fn new() -> Self {
        Self {
            frame_limit: NonZeroU32::new(16 * 1024).unwrap(),
            max_inbound_streams: 128,
            max_outbound_streams: 128,
            local_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            remote_recv_window: 64 * 1024,
            ping_interval: Duration::from_secs(5),
            global_recv_window: 4 * 1024 * 1024,
            local_transit_window: 0,
            remote_transit_window: 0,
            transit_window_max: 4 * 1024 * 1024,
            transit_min_rtt_filter: false,
            transit_bw_gate: false,
            transit_clean_probe: false,
            clean_rtt_sizing: false,
            stream_window_gain: 1.5,
            stream_window_growth: 2,
        }
    }
}

impl Default for RammuxConfig {
    fn default() -> Self {
        Self::new()
    }
}
