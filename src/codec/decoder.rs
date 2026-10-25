use std::fmt;

use tokio::time::Instant;

use crate::{
    buffer::{Buffer, Data},
    header::{Flags, Header, MAX_HEADER_LEN},
};

pub enum DecoderState {
    ReadingHeader {
        /// Bytes of the header read so far.
        filled: usize,
        /// Length of the whole header.
        ///
        /// Until the flags are read it is just the length of the flags.
        need: usize,
        /// Valid flags, once they are read.
        flags: Option<Flags>,
        buffer: [u8; MAX_HEADER_LEN],
    },
    ReadingData {
        header: Header,
        header_at: Instant,
        buffer: Buffer,
        filled: usize,
    },
}

impl Default for DecoderState {
    fn default() -> Self {
        Self::ReadingHeader {
            filled: 0,
            need: Flags::FLAGS_LEN,
            flags: None,
            buffer: [0; MAX_HEADER_LEN],
        }
    }
}

/// A frame read from the transport.
#[derive(Debug)]
pub enum DecodedFrame {
    Frame(InboundFrame),
    Terminate,
}

/// A frame other than `TERM`.
pub struct InboundFrame {
    pub header: Header,
    /// Payload of the frame, empty if there is no `DATA`.
    pub data: Data,
    /// Time when the whole header was read.
    ///
    /// This is the time to use for `PING`s and `PONG`s,
    /// as the payload can take long to arrive.
    pub header_at: Instant,
}

impl fmt::Debug for InboundFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboundFrame")
            .field("header", &self.header)
            .field("data", &self.data.as_ref().len())
            .finish_non_exhaustive()
    }
}
