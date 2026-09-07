//! rammux connection configuration.

use std::{fmt, num::NonZeroU32};

/// Role in a rammux connection.
///
/// The role decides two things: the pool of [`StreamId`](crate::StreamId)s
/// a side can use when starting a new stream, and which side pays for the
/// transit layer's link-clearing probe - the client does, as the transit
/// [`Initiator`](transit::Role::Initiator).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RammuxRole {
    /// Can initiate streams with even IDs. Initiates the transit probe.
    Client,
    /// Can initiate streams with odd IDs. Answers the transit probe.
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

impl From<RammuxRole> for transit::Role {
    fn from(role: RammuxRole) -> Self {
        match role {
            RammuxRole::Client => Self::Initiator,
            RammuxRole::Server => Self::Responder,
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
///
/// The transit layer's settings are not among them: each side announces the
/// window it grants, so the two sides need not be configured alike.
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
    /// Size of the global local receive window shared between all streams.
    ///
    /// This pool will be used for autotuning local receive windows of streams
    /// that are limited by flow control. Such streams will "borrow" window size from the pool,
    /// allowing the remote peer to spend less time waiting on window updates.
    ///
    /// This value is a local know and does not have to be negotiated.
    pub global_recv_window: usize,
    /// How the transit layer under this connection sizes the window it
    /// grants the peer.
    ///
    /// rammux runs over [`transit::Transit`], which bounds how much data is
    /// in flight between the peers and steers that bound from the queuing
    /// delay it observes. This is that layer's [`Sizing`](transit::Sizing):
    /// the window to start from, the most it may grow to, the cadence
    /// credit is returned at, and the rule that sizes it. The default is
    /// the tuned configuration; see [`transit::window`] for what each
    /// setting was measured to do.
    ///
    /// This value is a local knob and does not have to be negotiated: the
    /// window is announced to the peer, not assumed by it.
    pub transit_sizing: transit::Sizing,
    /// How many of the last probe's durations the transit layer waits
    /// before the next one.
    ///
    /// The transit layer measures the clean round trip with a link-clearing
    /// probe that pauses data output on both sides, and spaces the probes
    /// by their own duration so their cost stays a bounded share of the
    /// connection's time on any link. See
    /// [`transit::Config::probe_spacing`].
    ///
    /// Only the [`RammuxRole::Client`] probes, so this value only matters
    /// there. It is a local knob and does not have to be negotiated.
    pub transit_probe_spacing: f64,
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
    /// 4. [`Self::global_recv_window`] - 4mb
    /// 5. [`Self::transit_sizing`] - [`transit::Sizing::default`]
    /// 6. [`Self::transit_probe_spacing`] - [`transit::DEFAULT_PROBE_SPACING`]
    pub fn new() -> Self {
        Self {
            frame_limit: NonZeroU32::new(16 * 1024).unwrap(),
            max_inbound_streams: 128,
            max_outbound_streams: 128,
            local_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            remote_recv_window: 64 * 1024,
            global_recv_window: 4 * 1024 * 1024,
            transit_sizing: transit::Sizing::default(),
            transit_probe_spacing: transit::DEFAULT_PROBE_SPACING,
        }
    }

    /// The transit layer's configuration for a connection in `role`.
    pub(crate) fn transit_config(&self, role: RammuxRole) -> transit::Config {
        transit::Config {
            sizing: self.transit_sizing,
            probe_spacing: self.transit_probe_spacing,
            role: role.into(),
        }
    }
}

impl Default for RammuxConfig {
    fn default() -> Self {
        Self::new()
    }
}
