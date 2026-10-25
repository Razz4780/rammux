//! rammux connection configuration.

use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use thiserror::Error;

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
/// - [`RammuxConfig::remote_recv_pool`]
/// - [`RammuxConfig::local_recv_pool`]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RammuxConfig {
    /// Limit for the length of data that can be sent in a single `DATA` frame.
    ///
    /// # Negotiation
    ///
    /// This limit applies to both sides of the connection, has to be negotiated beforehand,
    /// and must match on both sides.
    pub frame_limit: NonZeroU16,
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
    /// Base receive window of every stream, `B` in `TRANSIT_CONTROL.md`.
    ///
    /// Every stream can have this many bytes of its inbound data unread by the application,
    /// no matter what the other streams do. It is fixed for the life of the connection,
    /// the receiver does not tune it. On top of it, streams share [`Self::local_recv_pool`].
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::remote_recv_window`].
    pub local_recv_window: NonZeroU32,
    /// Base receive window the peer grants to every stream.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::local_recv_window`].
    pub remote_recv_window: NonZeroU32,
    /// Size of the pool of receive window shared by all inbound streams.
    ///
    /// The unread inbound data of all streams can exceed their base windows
    /// by this many bytes at most. The memory used for inbound data is therefore at most
    /// `streams * local_recv_window + local_recv_pool`.
    ///
    /// The pool is the responsibility of the user, rammux uses the configured value as it is.
    /// Any size works, 0 included (streams run on their base windows). A pool or a window smaller than
    /// [`Self::frame_limit`] is used with smaller frames.
    /// The pool bounds the queue that the connection can build on the path, at about
    /// `(streams * window + pool) / bandwidth`.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::remote_recv_pool`].
    pub local_recv_pool: u32,
    /// Size of the pool of receive window shared by the inbound streams of the peer.
    ///
    /// # Negotiation
    ///
    /// This value has to be negotiated beforehand and
    /// must match the peer's [`RammuxConfig::local_recv_pool`].
    pub remote_recv_pool: u32,
    /// A `PING` is sent when this much `DATA` was written since the last one.
    ///
    /// Every `PING` is answered with a `PONG`, which gives the peer a round trip time sample.
    /// Several `PING`s can be in flight.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub ping_bytes: NonZeroU32,
    /// A `PING` is sent when this much time passed since the last one, no matter how much data was written.
    ///
    /// This keeps an idle connection alive (see [`RammuxConfig::inbound_frame_timeout`]),
    /// so it must be shorter than the timeout.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub ping_interval: Duration,
    /// Max delay between receiving inbound frames, can be used to detect dead peers.
    ///
    /// This is safe to use even with periods of no traffic,
    /// as rammux expects responses to `PING` frames sent on interval.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub inbound_frame_timeout: Duration,
    /// Size of the global local receive window shared between all streams.
    ///
    /// This pool will be used for autotuning local receive windows of streams
    /// that are limited by flow control. Such streams will "borrow" window size from the pool,
    /// allowing the remote peer to spend less time waiting on window updates.
    ///
    /// This value is a local knob and does not have to be negotiated.
    pub global_recv_window: usize,
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
    /// 4. [`Self::ping_bytes`] - 32kb
    /// 5. [`Self::ping_interval`] - 5s
    /// 6. [`Self::inbound_frame_timeout`] - 10s
    /// 7. [`Self::global_recv_window`] - 4mb
    /// 8. [`Self::local_recv_pool`] and [`Self::remote_recv_pool`] - 4mb
    pub const fn new() -> Self {
        Self {
            frame_limit: NonZeroU16::new(16 * 1024).unwrap(),
            max_inbound_streams: 128,
            max_outbound_streams: 128,
            local_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            remote_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            local_recv_pool: 4 * 1024 * 1024,
            remote_recv_pool: 4 * 1024 * 1024,
            ping_bytes: NonZeroU32::new(32 * 1024).unwrap(),
            ping_interval: Duration::from_secs(5),
            inbound_frame_timeout: Duration::from_secs(10),
            global_recv_window: 4 * 1024 * 1024,
        }
    }
}

impl RammuxConfig {
    /// Checks that this config can be used to run a connection.
    ///
    /// [`RammuxConnection::new`](crate::connection::RammuxConnection::new) calls this.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_inbound_streams > MAX_STREAMS_PER_SIDE {
            return Err(ConfigError::TooManyStreams {
                field: "max_inbound_streams",
                value: self.max_inbound_streams,
            });
        }
        if self.max_outbound_streams > MAX_STREAMS_PER_SIDE {
            return Err(ConfigError::TooManyStreams {
                field: "max_outbound_streams",
                value: self.max_outbound_streams,
            });
        }
        if self.ping_interval.is_zero() {
            return Err(ConfigError::ZeroPingInterval);
        }
        if self.ping_interval >= self.inbound_frame_timeout {
            return Err(ConfigError::PingIntervalTooLong {
                ping_interval: self.ping_interval,
                inbound_frame_timeout: self.inbound_frame_timeout,
            });
        }
        Ok(())
    }
}

/// How many streams each side of a connection can have at most.
///
/// Stream IDs are 16-bit, the lowest bit tells which side initiated the stream.
pub const MAX_STREAMS_PER_SIDE: u32 = 1 << 15;

/// Reason why a [`RammuxConfig`] cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A stream limit exceeds the number of IDs each side has.
    #[error("{field} is {value}, which exceeds the maximum of {MAX_STREAMS_PER_SIDE}")]
    TooManyStreams {
        /// Name of the offending field.
        field: &'static str,
        /// Configured value.
        value: u32,
    },
    /// `ping_interval` is zero: a `PING` would be due all the time.
    #[error("ping_interval must not be zero")]
    ZeroPingInterval,
    /// `PING`s would not be sent often enough to keep `inbound_frame_timeout` from firing on an idle connection.
    #[error(
        "ping_interval ({ping_interval:?}) must be shorter than inbound_frame_timeout ({inbound_frame_timeout:?})"
    )]
    PingIntervalTooLong {
        /// Configured ping interval.
        ping_interval: Duration,
        /// Configured timeout.
        inbound_frame_timeout: Duration,
    },
}

impl Default for RammuxConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::config::{ConfigError, MAX_STREAMS_PER_SIDE, RammuxConfig};

    #[test]
    fn default_is_valid() {
        RammuxConfig::new().validate().unwrap();
    }

    #[test]
    fn stream_limits_are_bounded() {
        let mut config = RammuxConfig::new();
        config.max_inbound_streams = MAX_STREAMS_PER_SIDE;
        config.max_outbound_streams = MAX_STREAMS_PER_SIDE;
        config.validate().unwrap();

        config.max_inbound_streams += 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyStreams {
                field: "max_inbound_streams",
                ..
            })
        ));

        config.max_inbound_streams = 1;
        config.max_outbound_streams += 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyStreams {
                field: "max_outbound_streams",
                ..
            })
        ));
    }

    #[test]
    fn ping_interval_is_shorter_than_timeout() {
        let mut config = RammuxConfig::new();
        config.ping_interval = config.inbound_frame_timeout;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::PingIntervalTooLong { .. })
        ));
        config.ping_interval = config.inbound_frame_timeout - Duration::from_millis(1);
        config.validate().unwrap();
    }

    #[test]
    fn ping_interval_is_not_zero() {
        let mut config = RammuxConfig::new();
        config.ping_interval = Duration::ZERO;
        assert_eq!(config.validate(), Err(ConfigError::ZeroPingInterval));
    }
}
