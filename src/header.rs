//! Types for encoding/decoding rammux frame headers.
//!
//! There is one frame type. A frame starts with 16 bits of flags,
//! followed by the header fields whose flags are set (in the fixed wire order), followed by the payload.
//! See `PROTOCOL.md`.

use std::num::{NonZeroU16, NonZeroU32};

use bitflags::bitflags;

use crate::{error::DecodeError, stream_id::StreamId};

bitflags! {
    /// Flags of a rammux frame.
    #[derive(PartialEq, Eq, Clone, Copy, Debug)]
    pub struct Flags: u16 {
        /// First frame of a stream.
        const SYN       = 0x0001;
        /// The sender of the frame stopped reading the stream.
        const FIN_READ  = 0x0002;
        /// The sender of the frame stopped writing the stream.
        const FIN_WRITE = 0x0004;
        /// The frame returns bytes of the receive window to the peer.
        const WINDOW    = 0x0008;
        /// The frame carries stream data.
        const DATA      = 0x0010;
        /// The frame carries a `PING`.
        const PING      = 0x0020;
        /// The frame carries a `PONG`.
        const PONG      = 0x0040;
        /// End of the rammux session.
        const TERM      = 0x0100;
    }
}

impl Flags {
    /// Flags that make the frame carry a stream ID.
    const STREAM: Self = Self::SYN
        .union(Self::FIN_READ)
        .union(Self::FIN_WRITE)
        .union(Self::WINDOW)
        .union(Self::DATA);

    /// Returns the length of the whole header with these flags.
    ///
    /// The flags must be [valid](Self::validate).
    pub const fn header_len(self) -> usize {
        let mut len = Self::FLAGS_LEN;
        if self.intersects(Self::STREAM) {
            len += 2;
        }
        if self.contains(Self::WINDOW) {
            len += 4;
        }
        if self.contains(Self::DATA) {
            len += 2;
        }
        if self.contains(Self::PING) {
            len += 4;
        }
        if self.contains(Self::PONG) {
            len += 12;
        }
        len
    }

    /// Length of the flags field itself.
    pub const FLAGS_LEN: usize = 2;

    /// Checks the flags in isolation.
    pub fn validate(raw: u16) -> Result<Self, DecodeError> {
        let Some(flags) = Self::from_bits(raw) else {
            return Err(DecodeError("reserved flags set"));
        };
        if flags.is_empty() {
            return Err(DecodeError("frame without flags"));
        }
        if flags.contains(Self::TERM) && flags != Self::TERM {
            return Err(DecodeError("TERM combined with other flags"));
        }
        Ok(flags)
    }
}

/// Length of the longest possible header.
pub const MAX_HEADER_LEN: usize = 26;

/// Fields of a `PONG`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pong {
    /// `t_ping` of the `PING` that is answered.
    pub t_echo: u32,
    /// Clock of the responder at the moment it read that `PING`.
    pub t_read: u32,
    /// Bytes of `DATA` the responder had read before it read that `PING`.
    pub rx_bytes: u32,
}

/// Control flags of a stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ControlFlags {
    pub fin_read: bool,
    pub fin_write: bool,
    pub syn: bool,
}

/// The part of a header that concerns one stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamHeader {
    /// ID of the stream.
    pub id: StreamId,
    /// Control flags.
    pub flags: ControlFlags,
    /// Bytes returned to the receive window of the peer.
    pub credit: Option<NonZeroU32>,
    /// Length of the payload.
    pub data_len: Option<NonZeroU16>,
}

impl StreamHeader {
    fn flags(&self) -> Flags {
        let mut flags = Flags::empty();
        flags.set(Flags::SYN, self.flags.syn);
        flags.set(Flags::FIN_READ, self.flags.fin_read);
        flags.set(Flags::FIN_WRITE, self.flags.fin_write);
        flags.set(Flags::WINDOW, self.credit.is_some());
        flags.set(Flags::DATA, self.data_len.is_some());
        flags
    }
}

/// A frame header, validated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Header {
    /// Stream part of the frame.
    pub stream: Option<StreamHeader>,
    /// `t_ping` of a `PING` carried by the frame.
    pub ping: Option<u32>,
    /// A `PONG` carried by the frame.
    pub pong: Option<Pong>,
    /// Whether this is the `TERM` frame.
    pub term: bool,
}

impl Header {
    /// Returns the flags of this header.
    ///
    /// The flags are the source of truth for which fields are encoded.
    pub fn flags(&self) -> Flags {
        let mut flags = self.stream.as_ref().map_or(Flags::empty(), |s| s.flags());
        flags.set(Flags::PING, self.ping.is_some());
        flags.set(Flags::PONG, self.pong.is_some());
        flags.set(Flags::TERM, self.term);
        flags
    }

    /// Encodes this header, returning the number of used bytes of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if the header is not a legal frame: it has no flags,
    /// it has a stream part without flags, or `TERM` is combined with something else.
    pub fn encode(&self, out: &mut [u8; MAX_HEADER_LEN]) -> usize {
        let flags = self.flags();
        assert!(
            Flags::validate(flags.bits()).is_ok(),
            "illegal frame: {flags:?}"
        );
        if let Some(stream) = &self.stream {
            assert!(!stream.flags().is_empty(), "stream part without flags");
        }

        let mut writer = Writer { buf: out, at: 0 };
        writer.put(&flags.bits().to_be_bytes());
        if let Some(stream) = &self.stream {
            writer.put(&stream.id.to_be_bytes());
            if let Some(credit) = stream.credit {
                writer.put(&credit.get().to_be_bytes());
            }
            if let Some(len) = stream.data_len {
                writer.put(&len.get().to_be_bytes());
            }
        }
        if let Some(t_ping) = self.ping {
            writer.put(&t_ping.to_be_bytes());
        }
        if let Some(pong) = &self.pong {
            writer.put(&pong.t_echo.to_be_bytes());
            writer.put(&pong.t_read.to_be_bytes());
            writer.put(&pong.rx_bytes.to_be_bytes());
        }
        debug_assert_eq!(writer.at, flags.header_len());
        writer.at
    }

    /// Decodes a header.
    ///
    /// `bytes` must be exactly `flags.header_len()` long, and start with the flags.
    /// The flags must be [valid](Flags::validate).
    /// Fails if the values of the fields are illegal.
    pub fn decode(flags: Flags, bytes: &[u8]) -> Result<Self, DecodeError> {
        debug_assert_eq!(bytes.len(), flags.header_len());
        let mut reader = Reader {
            buf: bytes,
            at: Flags::FLAGS_LEN,
        };

        let stream = if flags.intersects(Flags::STREAM) {
            let id = StreamId::from_be_bytes(reader.take());
            let credit = if flags.contains(Flags::WINDOW) {
                Some(
                    NonZeroU32::new(u32::from_be_bytes(reader.take()))
                        .ok_or(DecodeError("WINDOW with zero credit"))?,
                )
            } else {
                None
            };
            let data_len = if flags.contains(Flags::DATA) {
                Some(
                    NonZeroU16::new(u16::from_be_bytes(reader.take()))
                        .ok_or(DecodeError("DATA with zero length"))?,
                )
            } else {
                None
            };
            Some(StreamHeader {
                id,
                flags: ControlFlags {
                    fin_read: flags.contains(Flags::FIN_READ),
                    fin_write: flags.contains(Flags::FIN_WRITE),
                    syn: flags.contains(Flags::SYN),
                },
                credit,
                data_len,
            })
        } else {
            None
        };
        let ping = flags
            .contains(Flags::PING)
            .then(|| u32::from_be_bytes(reader.take()));
        let pong = flags.contains(Flags::PONG).then(|| Pong {
            t_echo: u32::from_be_bytes(reader.take()),
            t_read: u32::from_be_bytes(reader.take()),
            rx_bytes: u32::from_be_bytes(reader.take()),
        });
        debug_assert_eq!(reader.at, bytes.len());

        Ok(Self {
            stream,
            ping,
            pong,
            term: flags.contains(Flags::TERM),
        })
    }
}

struct Writer<'a> {
    buf: &'a mut [u8; MAX_HEADER_LEN],
    at: usize,
}

impl Writer<'_> {
    fn put(&mut self, bytes: &[u8]) {
        self.buf[self.at..self.at + bytes.len()].copy_from_slice(bytes);
        self.at += bytes.len();
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let out = self.buf[self.at..self.at + N]
            .try_into()
            .expect("slice has N bytes");
        self.at += N;
        out
    }
}

#[cfg(test)]
mod test {
    use std::num::{NonZeroU16, NonZeroU32};

    use rstest::rstest;

    use crate::{
        StreamId,
        header::{ControlFlags, Flags, Header, MAX_HEADER_LEN, Pong, StreamHeader},
    };

    fn stream(flags: ControlFlags, credit: Option<u32>, len: Option<u16>) -> Option<StreamHeader> {
        Some(StreamHeader {
            id: StreamId::from_be_bytes([0x12, 0x34]),
            flags,
            credit: credit.and_then(NonZeroU32::new),
            data_len: len.and_then(NonZeroU16::new),
        })
    }

    const PONG: Pong = Pong {
        t_echo: 0x0A0B0C0D,
        t_read: 0x01020304,
        rx_bytes: 0xF0F1F2F3,
    };

    #[test]
    fn longest_header_fits() {
        assert_eq!(
            Flags::all().difference(Flags::TERM).header_len(),
            MAX_HEADER_LEN
        );
    }

    #[test]
    fn layout() {
        let header = Header {
            stream: stream(
                ControlFlags {
                    fin_read: true,
                    fin_write: false,
                    syn: true,
                },
                Some(0x11223344),
                Some(0x5566),
            ),
            ping: Some(0x99AABBCC),
            pong: Some(PONG),
            term: false,
        };
        let mut buf = [0; MAX_HEADER_LEN];
        let len = header.encode(&mut buf);
        assert_eq!(
            &buf[..len],
            &[
                0x00, 0x7B, // SYN | FIN_READ | WINDOW | DATA | PING | PONG
                0x12, 0x34, // stream_id
                0x11, 0x22, 0x33, 0x44, // credit
                0x55, 0x66, // len
                0x99, 0xAA, 0xBB, 0xCC, // t_ping
                0x0A, 0x0B, 0x0C, 0x0D, // t_echo
                0x01, 0x02, 0x03, 0x04, // t_read
                0xF0, 0xF1, 0xF2, 0xF3, // rx_bytes
            ]
        );
        assert_eq!(len, MAX_HEADER_LEN);
    }

    #[rstest]
    #[case::term(Header { term: true, ..Default::default() }, &[0x01, 0x00])]
    #[case::ping(Header { ping: Some(7), ..Default::default() }, &[0x00, 0x20, 0, 0, 0, 7])]
    #[case::pong(
        Header { pong: Some(PONG), ..Default::default() },
        &[0x00, 0x40, 0x0A, 0x0B, 0x0C, 0x0D, 0x01, 0x02, 0x03, 0x04, 0xF0, 0xF1, 0xF2, 0xF3],
    )]
    #[case::fin_only(
        Header {
            stream: stream(ControlFlags { fin_read: true, fin_write: true, syn: false }, None, None),
            ..Default::default()
        },
        &[0x00, 0x06, 0x12, 0x34],
    )]
    #[case::data(
        Header {
            stream: stream(ControlFlags::default(), None, Some(3)),
            ..Default::default()
        },
        &[0x00, 0x10, 0x12, 0x34, 0x00, 0x03],
    )]
    #[case::window(
        Header {
            stream: stream(ControlFlags { syn: true, ..Default::default() }, Some(2137), None),
            ..Default::default()
        },
        &[0x00, 0x09, 0x12, 0x34, 0, 0, 0x08, 0x59],
    )]
    #[test]
    fn encode_decode(#[case] header: Header, #[case] expected: &[u8]) {
        let mut buf = [0; MAX_HEADER_LEN];
        let len = header.encode(&mut buf);
        assert_eq!(&buf[..len], expected);

        let raw_flags = u16::from_be_bytes([buf[0], buf[1]]);
        let flags = Flags::validate(raw_flags).unwrap();
        assert_eq!(flags, header.flags());
        assert_eq!(flags.header_len(), len);
        assert_eq!(Header::decode(flags, &buf[..len]).unwrap(), header);
    }

    #[test]
    fn every_legal_flag_combination_roundtrips() {
        // All 2^7 combinations of the non-TERM flags.
        let all = Flags::all().difference(Flags::TERM).bits();
        let mut legal = 0;
        for bits in 0..=all {
            let Some(flags) = Flags::from_bits(bits).filter(|f| !f.is_empty()) else {
                continue;
            };
            legal += 1;
            let header = Header {
                stream: flags
                    .intersects(
                        Flags::SYN
                            | Flags::FIN_READ
                            | Flags::FIN_WRITE
                            | Flags::WINDOW
                            | Flags::DATA,
                    )
                    .then(|| StreamHeader {
                        id: StreamId::from_be_bytes([0xFF, 0xFE]),
                        flags: ControlFlags {
                            fin_read: flags.contains(Flags::FIN_READ),
                            fin_write: flags.contains(Flags::FIN_WRITE),
                            syn: flags.contains(Flags::SYN),
                        },
                        credit: flags.contains(Flags::WINDOW).then_some(NonZeroU32::MIN),
                        data_len: flags.contains(Flags::DATA).then_some(NonZeroU16::MAX),
                    }),
                ping: flags.contains(Flags::PING).then_some(u32::MAX),
                pong: flags.contains(Flags::PONG).then_some(PONG),
                term: false,
            };
            let mut buf = [0; MAX_HEADER_LEN];
            let len = header.encode(&mut buf);
            assert_eq!(len, flags.header_len());
            let decoded_flags = Flags::validate(u16::from_be_bytes([buf[0], buf[1]])).unwrap();
            assert_eq!(decoded_flags, flags);
            assert_eq!(Header::decode(flags, &buf[..len]).unwrap(), header);
        }
        assert_eq!(legal, 127);
    }

    #[rstest]
    #[case::reserved_bit_0x0080(0x0080)]
    #[case::reserved(0x0200)]
    #[case::reserved_high(0x8001)]
    #[case::no_flags(0x0000)]
    #[case::term_and_ping(0x0120)]
    #[case::term_and_data(0x0110)]
    #[case::term_and_syn(0x0101)]
    #[test]
    fn invalid_flags(#[case] raw: u16) {
        Flags::validate(raw).unwrap_err();
    }

    #[test]
    fn invalid_values() {
        // WINDOW with credit 0.
        let flags = Flags::WINDOW;
        Header::decode(flags, &[0x00, 0x08, 0x00, 0x01, 0, 0, 0, 0]).unwrap_err();
        // DATA with len 0.
        let flags = Flags::DATA;
        Header::decode(flags, &[0x00, 0x10, 0x00, 0x01, 0, 0]).unwrap_err();
    }
}
