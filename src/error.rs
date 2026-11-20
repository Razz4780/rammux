//! rammux error types.

use std::io;

use thiserror::Error;

use crate::stream_id::StreamId;

/// Opaque error originating from a rammux connection.
#[derive(Error, Debug)]
#[error(transparent)]
pub struct RammuxError(#[from] pub(crate) ErrorKind);

impl RammuxError {
    /// Returns whether this error originates from misuse of this crate.
    pub fn is_user(&self) -> bool {
        match &self.0 {
            ErrorKind::AlreadyDowngraded | ErrorKind::Poisoned | ErrorKind::Config(..) => true,
            ErrorKind::Decode(..) | ErrorKind::Io(..) | ErrorKind::Stream { .. } => false,
        }
    }

    /// Returns whether this error originates from an IO failure in the underlying transport.
    pub fn is_io(&self) -> bool {
        matches!(&self.0, ErrorKind::Io(..))
    }

    /// If this error originates from an IO failure in the underlying transport, returns a reference to the source error.
    pub fn as_io(&self) -> Option<&io::Error> {
        if let ErrorKind::Io(error) = &self.0 {
            Some(error)
        } else {
            None
        }
    }

    /// If this error originates from an IO failure in the underlying transport, returns the source error.
    pub fn into_io(self) -> Result<io::Error, Self> {
        match self.0 {
            ErrorKind::Io(error) => Ok(error),
            other => Err(Self(other)),
        }
    }

    /// Returns whether this error originates from a protocol violation by the other side of the connection.
    pub fn is_protocol_violation(&self) -> bool {
        match &self.0 {
            ErrorKind::Decode(..) | ErrorKind::Stream { .. } => true,
            ErrorKind::Io(..)
            | ErrorKind::AlreadyDowngraded
            | ErrorKind::Poisoned
            | ErrorKind::Config(..) => false,
        }
    }
}

impl From<RammuxError> for io::Error {
    fn from(value: RammuxError) -> Self {
        match value.0 {
            ErrorKind::Io(error) => error,
            other => io::Error::other(other),
        }
    }
}

#[derive(Error, Debug)]
pub enum ErrorKind {
    /// IO transport failed.
    #[error("transport failed")]
    Io(#[from] io::Error),
    /// Failed to decode an inbound frame.
    #[error(transparent)]
    Decode(#[from] DecodeError),
    /// rammux protocol was violated within a specific stream.
    #[error("peer violated the protocol in stream {id}")]
    Stream {
        /// ID of the stream.
        id: StreamId,
        /// Message describing the protocol violation.
        #[source]
        error: StreamViolation,
    },
    /// rammux connection was downgraded and is no longer valid.
    #[error("connection already downgraded")]
    AlreadyDowngraded,
    #[error("connection poisoned")]
    Poisoned,
    /// A [`RammuxConfig`](crate::config::RammuxConfig) setting was rejected.
    #[error("invalid config: {0}")]
    Config(String),
}

impl From<io::ErrorKind> for ErrorKind {
    fn from(value: io::ErrorKind) -> Self {
        Self::Io(value.into())
    }
}

/// rammux protocol violations that can occur within a specific stream.
#[derive(Error, Debug)]
#[error("{0}")]
pub struct StreamViolation(pub &'static str);

/// Why a stream ended, if it did not end cleanly, or why its [`RammuxSink`] cannot take data.
///
/// The [`RammuxStream`] of a stream yields it after the data that arrived (after the data that was read,
/// for [`Self::PeerReset`]), and then the end of the stream.
/// The [`RammuxSink`] returns it from every call, once its direction ended.
///
/// [`RammuxSink`]: crate::stream::RammuxSink
/// [`RammuxStream`]: crate::stream::RammuxStream
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The peer dropped its sink without closing it. The data that arrived before was delivered.
    #[error("the peer stopped writing without closing the stream")]
    PeerAborted,
    /// The peer reset the stream. The data that was not read before the reset arrived was discarded.
    #[error("the peer reset the stream")]
    PeerReset,
    /// The peer dropped its reader. Nothing more can be written to the stream.
    #[error("the peer stopped reading the stream")]
    PeerStoppedReading,
    /// The connection failed: [`RammuxConnection::poll_progress`](crate::connection::RammuxConnection::poll_progress)
    /// returned an error.
    #[error("the connection failed")]
    ConnectionFailed,
    /// The connection was downgraded, by this side or by the peer.
    #[error("the connection was downgraded")]
    ConnectionDowngraded,
    /// The [`RammuxConnection`](crate::connection::RammuxConnection) was dropped.
    #[error("the connection was dropped")]
    ConnectionDropped,
    /// The sink was closed. Flushing and closing it again succeed, writing to it fails.
    #[error("the sink is closed")]
    SinkClosed,
    /// [`Sink::start_send`](futures::Sink::start_send) was called before
    /// [`Sink::poll_ready`](futures::Sink::poll_ready) returned [`Poll::Ready`](std::task::Poll::Ready).
    /// The data was not taken, and the sink can still be used.
    #[error("the sink is not ready")]
    SinkNotReady,
}

impl From<StreamError> for io::Error {
    fn from(value: StreamError) -> Self {
        let kind = match value {
            StreamError::PeerAborted | StreamError::PeerReset => io::ErrorKind::ConnectionReset,
            StreamError::PeerStoppedReading | StreamError::SinkClosed => io::ErrorKind::BrokenPipe,
            StreamError::ConnectionFailed
            | StreamError::ConnectionDowngraded
            | StreamError::ConnectionDropped => io::ErrorKind::ConnectionAborted,
            StreamError::SinkNotReady => io::ErrorKind::Other,
        };
        Self::new(kind, value)
    }
}

/// Errors that can occur when decoding an inbound rammux frame.
///
/// The message describes the framing violation.
#[derive(Error, Debug, Clone, Copy)]
#[error("received an invalid frame ({0})")]
pub struct DecodeError(pub &'static str);
