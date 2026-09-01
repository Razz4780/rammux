//! Turning inbound bytes into rammux frames.

use std::fmt;

use crate::{
    StreamId,
    buffer::{Buffer, Data},
    header::{ControlFlags, PingPayload, RawHeader},
};

/// Where the decoder stands in the inbound byte flow.
pub enum DecoderState {
    /// Filling the fixed-size header.
    ReadingHeader {
        /// Header bytes read so far.
        filled: usize,
        /// Partially filled header.
        buffer: [u8; RawHeader::LEN],
    },
    /// Filling the payload of a `DATA` frame whose header is decoded.
    ReadingData {
        /// Stream the payload belongs to.
        stream_id: StreamId,
        /// Lifecycle bits from the header, applied once the payload lands.
        flags: ControlFlags,
        /// Partially filled payload, sized to the header's `len`.
        buffer: Buffer,
        /// Payload bytes read so far.
        filled: usize,
    },
}

impl Default for DecoderState {
    fn default() -> Self {
        Self::ReadingHeader {
            filled: 0,
            buffer: [0; RawHeader::LEN],
        }
    }
}

/// A complete inbound frame.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodedFrame {
    /// An RTT measurement request or its response.
    Ping {
        /// Opaque payload the responder echoes back verbatim.
        payload: PingPayload,
        /// Whether this is the response rather than the request.
        is_response: bool,
    },
    /// Anything addressed to one virtual stream.
    Stream {
        /// Stream the frame belongs to.
        stream_id: StreamId,
        /// Lifecycle bits carried along.
        flags: ControlFlags,
        /// What the frame carries.
        payload: StreamPayload,
    },
    /// Credit returned for the session-level transit window.
    SessionWindowUpdate {
        /// Bytes of credit returned.
        update: u32,
    },
    /// Drain barrier of the link-clearing probe.
    ClearLink {
        /// Spontaneous initiation (`SYN` set) vs the responder's receipt.
        syn: bool,
    },
    /// The peer's end of the rammux session.
    Terminate,
}

/// What a stream-level frame carries.
#[derive(PartialEq, Eq)]
pub enum StreamPayload {
    /// Bytes of receive window credit returned to us.
    WindowUpdate(u32),
    /// Payload bytes for the stream's reader.
    Data(Data),
}

impl fmt::Debug for StreamPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WindowUpdate(update) => f.debug_tuple("WindowUpdate").field(update).finish(),
            Self::Data(data) => f.debug_tuple("Data").field(&data.as_ref().len()).finish(),
        }
    }
}
