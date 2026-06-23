//! Types for encoding/decoding Rammux frame headers.

use std::{fmt, ops::Not};

use bitflags::{Flags, bitflags};

use crate::{error::DecodeError, stream_id::StreamId};

/// 8-byte header of a Rammux frame, not validated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawHeader {
    /// ID of a Rammux stream.
    ///
    /// 1. For [`RawFlags::DATA`] and [`RawFlags::WINDOW_UPDATE`] frames,
    ///    this denotes the stream in which data is transferred.
    /// 2. For [`RawFlags::PING`] frames, this is the first part of the payload.
    /// 3. For [`RawFlags::TERM`] frames, this is `0`.
    pub stream_id: StreamId,
    /// Flags set on the frame.
    ///
    /// For [`RawFlags::TERM`] frames, the is [`RawFlags::empty`].
    pub flags: RawFlags,
    /// Len of the frame.
    ///
    /// 1. For [`RawFlags::DATA`] frames, this denotes the length of the data.
    /// 2. For [`RawFlags::WINDOW_UPDATE`] frames, this denotes the number of bytes returned to the receive window.
    /// 3. For [`RawFlags::PING`] frames, this is the second part of the payload.
    /// 4. For [`RawFlags::TERM`] frames, this is `0`.
    pub len: u32,
}

impl RawHeader {
    /// Length of the header in bytes.
    pub const LEN: usize = 8;

    /// Decodes the header from the given wire representation.
    ///
    /// This function does not verify the bytes in any way.
    pub const fn decode(raw: [u8; Self::LEN]) -> Self {
        let [s1, s2, s3, flags, l1, l2, l3, l4] = raw;
        let stream_id = StreamId::from_be_bytes([s1, s2, s3]);
        let flags = RawFlags::from_bits_retain(flags);
        let len = u32::from_be_bytes([l1, l2, l3, l4]);

        Self {
            stream_id,
            flags,
            len,
        }
    }

    /// Encodes the header to the wire representation.
    pub const fn encode(self) -> [u8; Self::LEN] {
        let [s1, s2, s3] = self.stream_id.to_be_bytes();
        let flags = self.flags.bits();
        let [l1, l2, l3, l4] = self.len.to_be_bytes();

        [s1, s2, s3, flags, l1, l2, l3, l4]
    }

    /// Validates this raw header, returning a clean enum.
    pub fn validate(self) -> Result<Header, DecodeError> {
        if self.flags.contains_unknown_bits() {
            return Err(DecodeError {
                header: self,
                message: "unknown flags set",
            });
        }

        let frame_type = self
            .flags
            .intersection(RawFlags::PING | RawFlags::WINDOW_UPDATE | RawFlags::DATA);
        match frame_type {
            RawFlags::PING => {
                if self
                    .flags
                    .intersects(RawFlags::FIN_READ | RawFlags::FIN_WRITE)
                {
                    Err(DecodeError {
                        header: self,
                        message: "PING frame with forbidden flags",
                    })
                } else {
                    Ok(Header::Ping {
                        payload: PingPayload {
                            stream_id: self.stream_id,
                            len: self.len,
                        },
                        is_response: self.flags.contains(RawFlags::SYN).not(),
                    })
                }
            },

            RawFlags::WINDOW_UPDATE => Ok(Header::WindowUpdate {
                stream_id: self.stream_id,
                flags: self.flags.into(),
                len: self.len,
            }),

            RawFlags::DATA => Ok(Header::Data {
                stream_id: self.stream_id,
                flags: self.flags.into(),
                len: self.len,
            }),

            RawFlags::TERM => {
                if u32::from(self.stream_id) == 0 && self.flags.is_empty() && self.len == 0 {
                    Ok(Header::Term)
                } else {
                    Err(DecodeError {
                        header: self,
                        message: "TERM frame with dirty bits",
                    })
                }
            },

            _ => Err(DecodeError {
                header: self,
                message: "invalid frame type",
            }),
        }
    }
}

impl fmt::LowerHex for RawHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let as_u64 = u64::from_be_bytes(self.encode());
        fmt::LowerHex::fmt(&as_u64, f)
    }
}

bitflags! {
    /// Flags of a Rammux frame.
    #[derive(PartialEq, Eq, Clone, Copy, Debug)]
    pub struct RawFlags: u8 {
        /// Denotes `PING` type of the frame.
        const PING          = 0b00000001;
        /// Denotes `WINDOW_UPDATE` type of the frame.
        const WINDOW_UPDATE = 0b00000010;
        /// Denotes `DATA` type of the frame.
        const DATA          = 0b00000100;
        /// Signals that the sender stopped reading from the stream.
        const FIN_READ      = 0b00001000;
        /// Signals that the sender stopped writing to the stream.
        const FIN_WRITE     = 0b00010000;
        /// Set on:
        /// 1. `PING` requests
        /// 2. `DATA` and `WINDOW_UPDATE` frames that initiate a new stream.
        const SYN           = 0b00100000;
    }
}

impl RawFlags {
    /// Flags for a term frame used in Rammux connection dowgrade.
    ///
    /// All bits unset.
    pub const TERM: Self = Self::empty();
}

/// Clean enum representing a valid Rammux frame header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Header {
    Ping {
        payload: PingPayload,
        is_response: bool,
    },
    WindowUpdate {
        stream_id: StreamId,
        flags: ControlFlags,
        len: u32,
    },
    Data {
        stream_id: StreamId,
        flags: ControlFlags,
        len: u32,
    },
    Term,
}

/// Control flags extracted from [`RawFlags`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ControlFlags {
    pub fin_read: bool,
    pub fin_write: bool,
    pub syn: bool,
}

impl From<RawFlags> for ControlFlags {
    fn from(value: RawFlags) -> Self {
        Self {
            fin_read: value.contains(RawFlags::FIN_READ),
            fin_write: value.contains(RawFlags::FIN_WRITE),
            syn: value.contains(RawFlags::SYN),
        }
    }
}

impl From<ControlFlags> for RawFlags {
    fn from(value: ControlFlags) -> Self {
        let mut flags = RawFlags::empty();
        if value.fin_read {
            flags |= RawFlags::FIN_READ;
        }
        if value.fin_write {
            flags |= RawFlags::FIN_WRITE;
        }
        if value.syn {
            flags |= RawFlags::SYN;
        }
        flags
    }
}

/// Payload of a [`RawFlags::PING`] frame.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PingPayload {
    /// In ping frames stream ID has no special meaning.
    /// It is used as the first 3 bytes of the payload.
    pub stream_id: StreamId,
    /// In ping frames frame length is used as the last 4 bytes of the payload.
    pub len: u32,
}

impl PingPayload {
    /// Generates a random payload using the thread-local RNG.
    pub fn random() -> Self {
        Self {
            stream_id: rand::random(),
            len: rand::random(),
        }
    }
}

impl fmt::Display for PingPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let composed = (u64::from(u32::from(self.stream_id)) << 32) | u64::from(self.len);
        write!(f, "{composed:#016x}")
    }
}

impl fmt::Debug for PingPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use crate::{
        StreamId,
        header::{ControlFlags, Header, PingPayload, RawFlags, RawHeader},
    };

    #[rstest]
    #[case(
        RawHeader { stream_id: StreamId::from_be_bytes([1, 2, 3]), flags: RawFlags::PING | RawFlags::SYN, len: 2137 },
        Some(Header::Ping {
            payload: PingPayload {
                stream_id: StreamId::from_be_bytes([1, 2, 3]),
                len: 2137,
            },
            is_response: false,
        }),
    )]
    #[case(
        RawHeader { stream_id: StreamId::from_be_bytes([4, 5, 6]), flags: RawFlags::PING, len: 444 },
        Some(Header::Ping {
            payload: PingPayload {
                stream_id: StreamId::from_be_bytes([4, 5, 6]),
                len: 444,
            },
            is_response: true,
        }),
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([7, 8, 9]),
            flags: RawFlags::WINDOW_UPDATE | RawFlags::FIN_READ | RawFlags::SYN,
            len: 99999,
        },
        Some(Header::WindowUpdate {
            stream_id: StreamId::from_be_bytes([7, 8, 9]),
            flags: ControlFlags {
                fin_read: true,
                fin_write: false,
                syn: true,
            },
            len: 99999,
        }),
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([7, 8, 9]),
            flags: RawFlags::DATA | RawFlags::FIN_WRITE,
            len: 88,
        },
        Some(Header::Data {
            stream_id: StreamId::from_be_bytes([7, 8, 9]),
            flags: ControlFlags {
                fin_read: false,
                fin_write: true,
                syn: false,
            },
            len: 88,
        }),
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([0, 0, 0]),
            flags: RawFlags::TERM,
            len: 0,
        },
        Some(Header::Term),
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([1, 2, 3]),
            flags: RawFlags::PING | RawFlags::SYN | RawFlags::FIN_READ,
            len: 2137,
        },
        None,
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([1, 2, 3]),
            flags: RawFlags::DATA | RawFlags::WINDOW_UPDATE,
            len: 2137,
        },
        None,
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([1, 0, 0]),
            flags: RawFlags::TERM,
            len: 0,
        },
        None,
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([0, 0, 0]),
            flags: RawFlags::SYN,
            len: 0,
        },
        None,
    )]
    #[case(
        RawHeader {
            stream_id: StreamId::from_be_bytes([0, 0, 0]),
            flags: RawFlags::TERM,
            len: 1,
        },
        None,
    )]
    #[test]
    fn decode_validate(#[case] decoded: RawHeader, #[case] validated: Option<Header>) {
        let encoded = decoded.encode();
        let decoded_again = RawHeader::decode(encoded);
        assert_eq!(decoded_again, decoded);

        match validated {
            Some(expected) => {
                let validated = decoded_again.validate().unwrap();
                assert_eq!(validated, expected);
            },
            None => {
                decoded_again.validate().unwrap_err();
            },
        }
    }
}
