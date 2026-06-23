use std::fmt;

use crate::{
    StreamId,
    buffer::{Buffer, Data},
    header::{ControlFlags, PingPayload, RawHeader},
};

pub enum DecoderState {
    ReadingHeader {
        filled: usize,
        buffer: [u8; RawHeader::LEN],
    },
    ReadingData {
        stream_id: StreamId,
        flags: ControlFlags,
        buffer: Buffer,
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

#[derive(Debug, PartialEq, Eq)]
pub enum DecodedFrame {
    Ping {
        payload: PingPayload,
        is_response: bool,
    },
    Stream {
        stream_id: StreamId,
        flags: ControlFlags,
        payload: StreamPayload,
    },
    Terminate,
}

#[derive(PartialEq, Eq)]
pub enum StreamPayload {
    WindowUpdate(u32),
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
