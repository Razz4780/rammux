//! Rammux errors types.

use std::{io, time::Duration};

use thiserror::Error;

use crate::{
    header::{PingPayload, RawHeader},
    stream_id::StreamId,
};

/// Opaque error originating from a Rammux connection.
#[derive(Error, Debug)]
#[error(transparent)]
pub struct RammuxError(#[from] pub(crate) ErrorKind);

impl RammuxError {
    /// Returns whether this error originates from misuse of this crate.
    pub fn is_user(&self) -> bool {
        matches!(&self.0, ErrorKind::AlreadyDowngraded | ErrorKind::Poisoned)
    }

    /// Returns whether this error originates from an IO failure in the supplied transport.
    pub fn is_io(&self) -> bool {
        matches!(&self.0, ErrorKind::Io(..))
    }

    /// If this error originates from an IO failure in the supplied transport, returns a reference to the source error.
    pub fn as_io(&self) -> Option<&io::Error> {
        if let ErrorKind::Io(error) = &self.0 {
            Some(error)
        } else {
            None
        }
    }

    /// If this error originates from an IO failure in the supplied transport, returns the source error.
    pub fn into_io(self) -> Result<io::Error, Self> {
        match self.0 {
            ErrorKind::Io(error) => Ok(error),
            other => Err(Self(other)),
        }
    }

    /// Returns whether this error originates from a protocol violation by the other side of the connection.
    pub fn is_protocol_violation(&self) -> bool {
        match &self.0 {
            ErrorKind::Decode(..) | ErrorKind::UnexpectedPing(..) | ErrorKind::Stream { .. } => {
                true
            },
            ErrorKind::Io(..)
            | ErrorKind::AlreadyDowngraded
            | ErrorKind::PingTimeout { .. }
            | ErrorKind::Poisoned => false,
        }
    }

    /// Returns whether this error originates from a ping timeout.
    pub fn is_ping_timeout(&self) -> bool {
        matches!(&self.0, ErrorKind::PingTimeout { .. })
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
    /// Rammux protocol was violated within a specific stream.
    #[error("peer violated the protocol in stream {id}")]
    Stream {
        /// ID of the stream.
        id: StreamId,
        /// Message describing the protocol violation.
        #[source]
        error: StreamError,
    },
    /// Received an unexpected `PING` frame.
    #[error("received an unexpected ping {0}")]
    UnexpectedPing(PingPayload),
    /// Failed to receive a `PING` response within the configured timeout.
    #[error("ping {payload} timed out after {:.02}s", elapsed.as_secs_f32())]
    PingTimeout {
        /// Payload of the failed `PING`.
        payload: PingPayload,
        /// Time elapsed since the `PING` was sent.
        elapsed: Duration,
    },
    /// Rammux connection was downgraded and is no longer valid.
    #[error("connection already downgraded")]
    AlreadyDowngraded,
    #[error("connection poisoned")]
    Poisoned,
}

impl From<io::ErrorKind> for ErrorKind {
    fn from(value: io::ErrorKind) -> Self {
        Self::Io(value.into())
    }
}

/// Rammux protocol violations that can occur within a specific stream.
#[derive(Error, Debug)]
#[error("{0}")]
pub struct StreamError(pub &'static str);

impl From<&'static str> for StreamError {
    fn from(value: &'static str) -> Self {
        Self(value)
    }
}

/// Errors that can occur when decoding an inbound Rammux frame.
#[derive(Error, Debug, Clone, Copy)]
#[error("received an invalid frame header {header:#018x} ({message})")]
pub struct DecodeError {
    /// Raw frame header that failed decoding.
    pub header: RawHeader,
    /// Opaque message.
    pub message: &'static str,
}
