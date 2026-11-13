//! rammux connection configuration.

use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use thiserror::Error;

/// Role in a rammux connection.
///
/// The two peers of a connection take opposite roles. The role only decides which
/// [`StreamId`](crate::StreamId)s a peer gives to the streams it starts.
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
/// [`RammuxConfig::new`] returns the defaults. The fields are public: change the ones you need.
///
/// # Settings that the peers have to agree on
///
/// rammux has no handshake, so each peer must know the settings of the other before the connection starts.
/// A connection whose peers do not agree fails, or loses throughput.
///
/// | This side | The peer |
/// | --- | --- |
/// | [`frame_limit`](Self::frame_limit) | the same `frame_limit` |
/// | [`max_outbound_streams`](Self::max_outbound_streams) | at most its `max_inbound_streams` |
/// | [`max_inbound_streams`](Self::max_inbound_streams) | at least its `max_outbound_streams` |
/// | [`local_recv_window`](Self::local_recv_window) | its `remote_recv_window` |
/// | [`remote_recv_window`](Self::remote_recv_window) | its `local_recv_window` |
/// | [`local_recv_pool`](Self::local_recv_pool) | its `remote_recv_pool` |
/// | [`remote_recv_pool`](Self::remote_recv_pool) | its `local_recv_pool` |
///
/// Two default configs match each other. All other settings are local: each side chooses them freely.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RammuxConfig {
    /// The most data that is sent in one frame, in bytes. Larger writes are split.
    ///
    /// Default: 16 KiB. Must be the same on both sides.
    pub frame_limit: NonZeroU16,
    /// How many streams started by the peer can be open at a time.
    ///
    /// The connection fails if the peer starts more.
    ///
    /// Default: 128. At most [`MAX_STREAMS_PER_SIDE`], and not less than the peer's
    /// [`max_outbound_streams`](Self::max_outbound_streams).
    pub max_inbound_streams: u32,
    /// How many streams started by this side can be open at a time.
    ///
    /// At the limit, [`try_start_outbound`](crate::connection::RammuxConnection::try_start_outbound)
    /// returns [`None`] until a stream is closed by both sides.
    ///
    /// Default: 128. At most [`MAX_STREAMS_PER_SIDE`], and not more than the peer's
    /// [`max_inbound_streams`](Self::max_inbound_streams).
    pub max_outbound_streams: u32,
    /// Receive window of every stream, in bytes.
    ///
    /// This much data of a stream can wait for the application to read it, whatever the other streams do.
    /// Streams that need more share [`local_recv_pool`](Self::local_recv_pool). The memory that the connection
    /// uses for received data is at most `streams * local_recv_window + local_recv_pool`.
    ///
    /// Default: 64 KiB. Must be the peer's [`remote_recv_window`](Self::remote_recv_window).
    pub local_recv_window: NonZeroU32,
    /// The peer's [`local_recv_window`](Self::local_recv_window).
    ///
    /// Default: 64 KiB.
    pub remote_recv_window: NonZeroU32,
    /// Receive memory that all streams share on top of their windows, in bytes.
    ///
    /// The pool is what lets busy streams go fast, and it is what limits the queue that the connection builds.
    /// A connection carries at most about `(streams * window + pool) / round trip time`, and delays an interactive
    /// stream by up to `(streams * window + pool) / bandwidth`. A pool of about the bandwidth times the round
    /// trip time is enough for full speed. Any size works, 0 included.
    ///
    /// Default: 4 MiB. Must be the peer's [`remote_recv_pool`](Self::remote_recv_pool).
    pub local_recv_pool: u32,
    /// The peer's [`local_recv_pool`](Self::local_recv_pool).
    ///
    /// Default: 4 MiB.
    pub remote_recv_pool: u32,
    /// How much of the peer's pool a new or idle stream may use at once, as a fraction of
    /// [`remote_recv_pool`](Self::remote_recv_pool).
    ///
    /// A stream normally earns its part of the pool: it starts with the peer's window, and may send more as
    /// the peer's application proves to read, about twice as much every round trip. The advance lets a stream
    /// that starts, or resumes after a pause, send a burst of the window plus this grant right away.
    ///
    /// A larger grant means faster bursts, and more that a stream whose reader never reads can hold on to.
    /// See [`advance_budget`](Self::advance_budget) for the limit on that.
    ///
    /// Default: 0.25. From 0 to `advance_budget`. With 0, there is no advance. This setting is local.
    pub advance_grant: f32,
    /// How much of the peer's pool all streams together may hold as an advance, as a fraction of
    /// [`remote_recv_pool`](Self::remote_recv_pool).
    ///
    /// This is the most that streams whose readers never read can take away from the others, on top of their
    /// windows. Once it is used up, new streams start without an advance.
    ///
    /// Default: 0.25. From [`advance_grant`](Self::advance_grant) to 1. This setting is local.
    pub advance_budget: f32,
    /// A `PING` is sent after this many bytes of data.
    ///
    /// The answers measure the round trip time, which the flow control needs while data flows.
    ///
    /// Default: 32 KiB. This setting is local.
    pub ping_bytes: NonZeroU32,
    /// A `PING` is sent at least this often, which keeps an idle connection alive.
    ///
    /// Default: 5 s. Not zero, and shorter than [`inbound_frame_timeout`](Self::inbound_frame_timeout).
    /// This setting is local.
    pub ping_interval: Duration,
    /// The connection fails when nothing arrives from the peer for this long.
    ///
    /// The peer answers `PING`s, so a connection that is idle and healthy does not time out.
    ///
    /// Default: 10 s. This setting is local.
    pub inbound_frame_timeout: Duration,
}

impl RammuxConfig {
    /// Creates a config with the default settings, which the docs of the fields list.
    ///
    /// Two default configs match each other. If you change a setting that the peers have to agree on,
    /// change it on both sides.
    pub const fn new() -> Self {
        Self {
            frame_limit: NonZeroU16::new(16 * 1024).unwrap(),
            max_inbound_streams: 128,
            max_outbound_streams: 128,
            local_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            remote_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            local_recv_pool: 4 * 1024 * 1024,
            remote_recv_pool: 4 * 1024 * 1024,
            advance_grant: 0.25,
            advance_budget: 0.25,
            ping_bytes: NonZeroU32::new(32 * 1024).unwrap(),
            ping_interval: Duration::from_secs(5),
            inbound_frame_timeout: Duration::from_secs(10),
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
        for (field, value) in [
            ("advance_grant", self.advance_grant),
            ("advance_budget", self.advance_budget),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(ConfigError::AdvanceNotAFraction { field });
            }
        }
        if self.advance_grant > self.advance_budget {
            return Err(ConfigError::AdvanceGrantAboveBudget);
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
    /// An advance setting is not a number from 0 to 1 (NaN and infinities included).
    #[error("{field} must be a finite fraction from 0 to 1")]
    AdvanceNotAFraction {
        /// Name of the offending field.
        field: &'static str,
    },
    /// `advance_grant` exceeds `advance_budget`.
    #[error("advance_grant must not exceed advance_budget")]
    AdvanceGrantAboveBudget,
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
    fn advance_is_a_pair_of_fractions_with_grant_up_to_budget() {
        let mut config = RammuxConfig::new();
        config.advance_grant = 1.0 / 16.0;
        config.advance_budget = 0.25;
        config.validate().unwrap();
        config.advance_grant = 0.25;
        config.validate().unwrap();
        config.advance_budget = 1.0;
        config.validate().unwrap();

        config.advance_grant = 0.5;
        config.advance_budget = 0.25;
        assert_eq!(config.validate(), Err(ConfigError::AdvanceGrantAboveBudget));

        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.1, 1.1] {
            let mut config = RammuxConfig::new();
            config.advance_grant = bad;
            assert_eq!(
                config.validate(),
                Err(ConfigError::AdvanceNotAFraction {
                    field: "advance_grant"
                })
            );
            let mut config = RammuxConfig::new();
            config.advance_budget = bad;
            assert_eq!(
                config.validate(),
                Err(ConfigError::AdvanceNotAFraction {
                    field: "advance_budget"
                })
            );
        }
    }

    #[test]
    fn ping_interval_is_not_zero() {
        let mut config = RammuxConfig::new();
        config.ping_interval = Duration::ZERO;
        assert_eq!(config.validate(), Err(ConfigError::ZeroPingInterval));
    }
}
