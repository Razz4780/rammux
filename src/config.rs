//! Settings of a rammux connection.

use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use crate::error::{ErrorKind, RammuxError};

/// The role of a peer in a rammux connection.
///
/// The two peers must take opposite roles. The role only decides which [`StreamId`](crate::StreamId)s
/// can be used when starting a new stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RammuxRole {
    /// Starts streams with even IDs.
    Client,
    /// Starts streams with odd IDs.
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

/// Settings of a [`RammuxConnection`](crate::connection::RammuxConnection).
///
/// Start from the defaults and change what you need:
///
/// ```
/// # use std::num::NonZeroU16;
/// # use rammux::config::RammuxConfig;
/// # fn main() -> Result<(), rammux::RammuxError> {
/// let config = RammuxConfig::default()
///     .with_frame_limit(NonZeroU16::new(8 * 1024).unwrap())
///     .with_stream_advance_grant(0.1)?
///     .with_remote_recv_pool(1024 * 1024);
/// assert_eq!(config.remote_recv_pool(), 1024 * 1024);
/// # Ok(())
/// # }
/// ```
///
/// # Settings that the peers have to agree on
///
/// rammux has no handshake. Each peer must know **some** of the other's settings before the connection starts.
/// Those settings are protocol-level limits. If the configured limits don't match,
/// [`RammuxConnection`](crate::connection::RammuxConnection) might breach them at runtime, triggering
/// a protocol violation error on any side.
///
/// * [`frame_limit`](Self::with_frame_limit) must be the same on both sides
/// * [`max_outbound_streams`](Self::with_max_outbound_streams) must be less or equal to
///   [`max_inbound_streams`](Self::with_max_inbound_streams) of the other side
/// * [`local_recv_window`](Self::with_local_recv_window) must be the same as
///   [`remote_recv_window`](Self::with_remote_recv_window) of the other side
/// * [`local_recv_pool`](Self::with_local_recv_pool) must be the same as
///   [`remote_recv_pool`](Self::with_remote_recv_pool) of the other side
///
/// The other settings are local knobs.
///
/// Two default configs always match.
#[derive(Debug, Clone)]
pub struct RammuxConfig {
    pub(crate) frame_limit: NonZeroU16,
    pub(crate) max_inbound_streams: u16,
    pub(crate) max_outbound_streams: u16,
    pub(crate) local_recv_window: NonZeroU32,
    pub(crate) remote_recv_window: NonZeroU32,
    pub(crate) local_recv_pool: u32,
    pub(crate) remote_recv_pool: u32,
    pub(crate) stream_advance_grant: f32,
    pub(crate) conn_advance_grant: f32,
    pub(crate) ping_interval: Duration,
    pub(crate) inbound_frame_timeout: Duration,
}

impl RammuxConfig {
    /// Creates a config with the default settings, same as [`RammuxConfig::default`].
    ///
    /// Two default configs match. If you change a setting that the peers have to agree on,
    /// change it on both sides. See this struct's docs.
    pub const fn new() -> Self {
        Self {
            frame_limit: NonZeroU16::new(16 * 1024).unwrap(),
            max_inbound_streams: 128,
            max_outbound_streams: 128,
            local_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            remote_recv_window: NonZeroU32::new(64 * 1024).unwrap(),
            local_recv_pool: 4 * 1024 * 1024,
            remote_recv_pool: 4 * 1024 * 1024,
            stream_advance_grant: 0.25,
            conn_advance_grant: 0.25,
            ping_interval: Duration::from_secs(5),
            inbound_frame_timeout: Duration::from_secs(10),
        }
    }

    /// Sets the largest allowed frame payload, in bytes.
    ///
    /// Larger writes are split into several frames.
    ///
    /// Default: 16 KiB. Must be the same on both sides.
    pub const fn with_frame_limit(mut self, frame_limit: NonZeroU16) -> Self {
        self.frame_limit = frame_limit;
        self
    }

    /// Sets how many streams the peer can have open at once.
    ///
    /// If the peer opens more, the connection fails.
    ///
    /// Default: 128. At most [`MAX_STREAMS_PER_SIDE`]. Must be at least the peer's
    /// [`max_outbound_streams`](Self::with_max_outbound_streams).
    pub fn with_max_inbound_streams(mut self, streams: u16) -> Result<Self, RammuxError> {
        self.max_inbound_streams = check_streams("max_inbound_streams", streams)?;
        Ok(self)
    }

    /// Sets how many streams this side can have open at once.
    ///
    /// At the limit, [`try_start_outbound`](crate::connection::RammuxConnection::try_start_outbound)
    /// returns [`None`]. It works again once both sides have closed one of the streams.
    ///
    /// Default: 128. At most [`MAX_STREAMS_PER_SIDE`]. Must be at most the peer's
    /// [`max_inbound_streams`](Self::with_max_inbound_streams).
    pub fn with_max_outbound_streams(mut self, streams: u16) -> Result<Self, RammuxError> {
        self.max_outbound_streams = check_streams("max_outbound_streams", streams)?;
        Ok(self)
    }

    /// Sets the receive window of each stream, in bytes.
    ///
    /// Every stream can always have this much data waiting to be read, no matter what the other streams do.
    /// To go faster, streams also borrow from the shared [`local_recv_pool`](Self::with_local_recv_pool).
    ///
    /// Received data takes at most `streams * local_recv_window + local_recv_pool` bytes of memory.
    ///
    /// Default: 64 KiB. Must equal the peer's [`remote_recv_window`](Self::with_remote_recv_window).
    pub const fn with_local_recv_window(mut self, window: NonZeroU32) -> Self {
        self.local_recv_window = window;
        self
    }

    /// Sets the peer's receive window, in bytes.
    ///
    /// Default: 64 KiB. Must equal the peer's [`local_recv_window`](Self::with_local_recv_window).
    pub const fn with_remote_recv_window(mut self, window: NonZeroU32) -> Self {
        self.remote_recv_window = window;
        self
    }

    /// Sets the receive pool that all streams share, in bytes.
    ///
    /// The pool is extra memory on top of the windows. Busy streams borrow from it to go faster than
    /// [`local_recv_window`](Self::with_local_recv_window) alone allows.
    ///
    /// The connection carries at most about `(streams * window + pool) / RTT`. For full speed, make the pool
    /// about the bandwidth times the RTT. Any size works, 0 included.
    ///
    /// Default: 4 MiB. Must equal the peer's [`remote_recv_pool`](Self::with_remote_recv_pool).
    pub const fn with_local_recv_pool(mut self, pool: u32) -> Self {
        self.local_recv_pool = pool;
        self
    }

    /// Sets the peer's receive pool, in bytes.
    ///
    /// Default: 4 MiB. Must equal the peer's [`local_recv_pool`](Self::with_local_recv_pool).
    pub const fn with_remote_recv_pool(mut self, pool: u32) -> Self {
        self.remote_recv_pool = pool;
        self
    }

    /// Sets how much of the peer's pool a stream can borrow right away, as a fraction of
    /// [`remote_recv_pool`](Self::with_remote_recv_pool).
    ///
    /// A stream normally earns its share of the pool. It starts with just its window. As the peer's
    /// application reads, the stream may borrow more, growing about 2x every round trip.
    ///
    /// The advance skips this wait. A new stream, or one that resumes after a pause, can borrow this much
    /// right from the start. A bigger advance gives faster bursts. But if nobody reads the stream, the borrowed
    /// memory stays stuck until the stream is closed. Stuck share of the pool limits the throughput available
    /// to other streams.
    ///
    /// Default: 0.25. From 0 to 1, and 0 turns the advance off. If you set it above
    /// [`conn_advance_grant`](Self::with_conn_advance_grant), that setting goes up to match.
    /// Local setting.
    pub fn with_stream_advance_grant(mut self, grant: f32) -> Result<Self, RammuxError> {
        self.stream_advance_grant = check_fraction("stream_advance_grant", grant)?;
        self.conn_advance_grant = self.conn_advance_grant.max(grant);
        Ok(self)
    }

    /// Sets how much of the peer's pool all streams together can hold as advances, as a fraction of
    /// [`remote_recv_pool`](Self::with_remote_recv_pool).
    ///
    /// This caps how much memory can get stuck in streams that nobody reads. Once the cap is reached,
    /// new streams start without an advance.
    ///
    /// Default: 0.25. From 0 to 1. If you set it below
    /// [`stream_advance_grant`](Self::with_stream_advance_grant), that setting goes down to match.
    /// Local setting.
    pub fn with_conn_advance_grant(mut self, grant: f32) -> Result<Self, RammuxError> {
        self.conn_advance_grant = check_fraction("conn_advance_grant", grant)?;
        self.stream_advance_grant = self.stream_advance_grant.min(grant);
        Ok(self)
    }

    /// Sets the longest time between two `PING`s.
    ///
    /// The peer answers every `PING`, which keeps an idle connection alive. Keep the interval shorter than
    /// [`inbound_frame_timeout`](Self::with_inbound_frame_timeout).
    ///
    /// Default: 5s. Must not be zero. Local setting.
    pub fn with_ping_interval(mut self, interval: Duration) -> Result<Self, RammuxError> {
        self.ping_interval = check_non_zero_duration("ping_interval", interval)?;
        Ok(self)
    }

    /// Sets how long the peer can stay silent before the connection fails.
    ///
    /// The peer answers `PING`s, so a healthy idle connection does not time out.
    ///
    /// Default: 10s. Must not be zero. Local setting.
    pub fn with_inbound_frame_timeout(mut self, timeout: Duration) -> Result<Self, RammuxError> {
        self.inbound_frame_timeout = check_non_zero_duration("inbound_frame_timeout", timeout)?;
        Ok(self)
    }

    /// See [`with_frame_limit`](Self::with_frame_limit).
    pub const fn frame_limit(&self) -> NonZeroU16 {
        self.frame_limit
    }

    /// See [`with_max_inbound_streams`](Self::with_max_inbound_streams).
    pub const fn max_inbound_streams(&self) -> u16 {
        self.max_inbound_streams
    }

    /// See [`with_max_outbound_streams`](Self::with_max_outbound_streams).
    pub const fn max_outbound_streams(&self) -> u16 {
        self.max_outbound_streams
    }

    /// See [`with_local_recv_window`](Self::with_local_recv_window).
    pub const fn local_recv_window(&self) -> NonZeroU32 {
        self.local_recv_window
    }

    /// See [`with_remote_recv_window`](Self::with_remote_recv_window).
    pub const fn remote_recv_window(&self) -> NonZeroU32 {
        self.remote_recv_window
    }

    /// See [`with_local_recv_pool`](Self::with_local_recv_pool).
    pub const fn local_recv_pool(&self) -> u32 {
        self.local_recv_pool
    }

    /// See [`with_remote_recv_pool`](Self::with_remote_recv_pool).
    pub const fn remote_recv_pool(&self) -> u32 {
        self.remote_recv_pool
    }

    /// See [`with_stream_advance_grant`](Self::with_stream_advance_grant).
    pub const fn stream_advance_grant(&self) -> f32 {
        self.stream_advance_grant
    }

    /// See [`with_conn_advance_grant`](Self::with_conn_advance_grant).
    pub const fn conn_advance_grant(&self) -> f32 {
        self.conn_advance_grant
    }

    /// See [`with_ping_interval`](Self::with_ping_interval).
    pub const fn ping_interval(&self) -> Duration {
        self.ping_interval
    }

    /// See [`with_inbound_frame_timeout`](Self::with_inbound_frame_timeout).
    pub const fn inbound_frame_timeout(&self) -> Duration {
        self.inbound_frame_timeout
    }

    /// Checks the invariants that the setters keep.
    ///
    /// The crate's tests set the fields directly, so
    /// [`RammuxConnection::new`](crate::connection::RammuxConnection::new) asserts this in debug builds.
    pub(crate) fn validate(&self) -> Result<(), RammuxError> {
        check_streams("max_inbound_streams", self.max_inbound_streams)?;
        check_streams("max_outbound_streams", self.max_outbound_streams)?;
        check_fraction("stream_advance_grant", self.stream_advance_grant)?;
        check_fraction("conn_advance_grant", self.conn_advance_grant)?;
        check_non_zero_duration("ping_interval", self.ping_interval)?;
        check_non_zero_duration("inbound_frame_timeout", self.inbound_frame_timeout)?;
        if self.stream_advance_grant > self.conn_advance_grant {
            return Err(config_error(format!(
                "stream_advance_grant ({}) must not exceed conn_advance_grant ({})",
                self.stream_advance_grant, self.conn_advance_grant
            )));
        }
        Ok(())
    }
}

impl Default for RammuxConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// The highest stream limit a side can have.
///
/// Stream IDs have 16 bits, and the lowest bit says which side started the stream.
pub const MAX_STREAMS_PER_SIDE: u16 = 1 << 15;

fn config_error(message: String) -> RammuxError {
    ErrorKind::Config(message).into()
}

fn check_streams(setting: &str, streams: u16) -> Result<u16, RammuxError> {
    if streams > MAX_STREAMS_PER_SIDE {
        return Err(config_error(format!(
            "{setting} is {streams}, which exceeds the maximum of {MAX_STREAMS_PER_SIDE}"
        )));
    }
    Ok(streams)
}

fn check_fraction(setting: &str, value: f32) -> Result<f32, RammuxError> {
    if !(0.0..=1.0).contains(&value) {
        return Err(config_error(format!(
            "{setting} is {value}, which is not a fraction from 0 to 1"
        )));
    }
    Ok(value)
}

fn check_non_zero_duration(setting: &str, interval: Duration) -> Result<Duration, RammuxError> {
    if interval.is_zero() {
        return Err(config_error(format!("{setting} must not be zero")));
    }
    Ok(interval)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::config::{MAX_STREAMS_PER_SIDE, RammuxConfig};

    #[test]
    fn default_is_valid() {
        RammuxConfig::default().validate().unwrap();
    }

    #[test]
    fn stream_limits_are_bounded() {
        let config = RammuxConfig::default()
            .with_max_inbound_streams(MAX_STREAMS_PER_SIDE)
            .unwrap()
            .with_max_outbound_streams(MAX_STREAMS_PER_SIDE)
            .unwrap();
        config.validate().unwrap();

        let error = config
            .clone()
            .with_max_inbound_streams(MAX_STREAMS_PER_SIDE + 1)
            .unwrap_err();
        assert!(error.is_user());
        assert!(error.to_string().contains("max_inbound_streams"), "{error}");

        let error = config
            .with_max_outbound_streams(MAX_STREAMS_PER_SIDE + 1)
            .unwrap_err();
        assert!(
            error.to_string().contains("max_outbound_streams"),
            "{error}"
        );
    }

    #[test]
    fn advance_grants_are_fractions_with_stream_up_to_conn() {
        let config = RammuxConfig::default()
            .with_stream_advance_grant(1.0 / 16.0)
            .unwrap()
            .with_conn_advance_grant(0.25)
            .unwrap();
        config.validate().unwrap();
        let config = config.with_stream_advance_grant(0.25).unwrap();
        config.validate().unwrap();
        let config = config.with_conn_advance_grant(1.0).unwrap();
        config.validate().unwrap();

        // The stream grant raises the connection grant, and the connection grant lowers the stream grant.
        let config = config.with_conn_advance_grant(0.25).unwrap();
        let config = config.with_stream_advance_grant(0.5).unwrap();
        assert_eq!(
            (config.stream_advance_grant(), config.conn_advance_grant()),
            (0.5, 0.5)
        );
        config.validate().unwrap();
        let config = config.with_conn_advance_grant(0.125).unwrap();
        assert_eq!(
            (config.stream_advance_grant(), config.conn_advance_grant()),
            (0.125, 0.125)
        );
        config.validate().unwrap();
        // A setting that does not cross the other leaves it alone.
        let config = config.with_conn_advance_grant(0.75).unwrap();
        assert_eq!(
            (config.stream_advance_grant(), config.conn_advance_grant()),
            (0.125, 0.75)
        );
        let config = config.with_stream_advance_grant(0.0).unwrap();
        assert_eq!(
            (config.stream_advance_grant(), config.conn_advance_grant()),
            (0.0, 0.75)
        );

        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.1, 1.1] {
            let error = RammuxConfig::default()
                .with_stream_advance_grant(bad)
                .unwrap_err();
            assert!(
                error.to_string().contains("stream_advance_grant"),
                "{error}"
            );
            let error = RammuxConfig::default()
                .with_conn_advance_grant(bad)
                .unwrap_err();
            assert!(error.to_string().contains("conn_advance_grant"), "{error}");
        }
    }

    #[test]
    fn ping_interval_is_not_zero() {
        let error = RammuxConfig::default()
            .with_ping_interval(Duration::ZERO)
            .unwrap_err();
        assert!(error.to_string().contains("ping_interval"), "{error}");
    }

    #[test]
    fn inbound_frame_timeout_is_not_zero() {
        let error = RammuxConfig::default()
            .with_inbound_frame_timeout(Duration::ZERO)
            .unwrap_err();
        assert!(
            error.to_string().contains("inbound_frame_timeout"),
            "{error}"
        );
    }
}
