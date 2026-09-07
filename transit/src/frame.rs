//! Wire format.
//!
//! Every frame is a fixed 8 byte header; `DATA` alone carries a timestamp and a
//! payload after it. The header is small on purpose. Returning credit finely
//! enough to keep a sender fed is the single highest-value property this
//! protocol has - see [`crate::window`] - and it is only affordable because a
//! re-grant costs 8 bytes. At the finest cadence the window ever asks for, that
//! is 0.2% of the link; at the coarsest, 0.01%.
//!
//! The two `DATA` extras exist for one rule each, and both are cheap enough to
//! carry unconditionally rather than negotiate: a 4 byte timestamp for the
//! one-way delay signal, and a flag bit, in a byte the header had spare, for
//! the sender to admit it ran out of credit.

use std::io;

use bytes::{BufMut, BytesMut};

/// Size of a frame header.
pub(crate) const HEADER_LEN: usize = 8;

/// Largest payload a single `DATA` frame may carry.
pub const MAX_PAYLOAD: u32 = 64 * 1024;

/// Bytes of sender timestamp carried between a `DATA` header and its payload.
///
/// Only `DATA` carries one, so control frames stay at [`HEADER_LEN`]. Four
/// bytes of microseconds wrap every 71 minutes; a connection outliving that
/// sees one corrupted one-way delay sample per wrap, which the receiver's
/// minimum filter discards.
pub(crate) const TIMESTAMP_LEN: usize = 4;

/// A frame header.
///
/// The payload of a `DATA` frame follows the header on the wire; every other
/// frame is the header alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Frame {
    /// A [`TIMESTAMP_LEN`] byte sender timestamp follows, then `len` payload
    /// bytes.
    Data {
        len: u32,
        /// Whether the sender had run out of credit before writing this.
        ///
        /// Growth is evidence-driven, and this is the evidence: a window
        /// nobody is filling buys nothing by getting bigger. Without it a
        /// direction that is idle - the echo side of this benchmark, which
        /// can only return what the forward side delivers - reads no queue,
        /// and a delay-targeting rule grows into the cap.
        starved: bool,
    },
    /// The peer may put `value` more bytes in flight.
    WindowUpdate(u32),
    /// Start of a link-clearing probe: the sender has paused its data output,
    /// so this frame arrives once the path towards the receiver has drained.
    ClearLink(u32),
    /// The probe's peer has paused its data output too. Reaching the initiator
    /// means the reverse path has drained as well, and the link is now empty.
    ClearAck(u32),
    /// Timed over the drained link.
    Ping(u32),
    Pong(u32),
    /// The clean round trip the initiator just measured, in microseconds.
    ///
    /// Both ends size their window from it, and only one end pays for the
    /// probe, so the measurement is shared rather than taken twice.
    CleanRtt(u32),
}

impl Frame {
    const KIND_DATA: u8 = 1;
    const KIND_WINDOW_UPDATE: u8 = 2;
    const KIND_CLEAR_LINK: u8 = 3;
    const KIND_CLEAR_ACK: u8 = 4;
    const KIND_PING: u8 = 5;
    const KIND_PONG: u8 = 6;
    const KIND_CLEAN_RTT: u8 = 7;

    /// Set on `DATA` written by a sender that had run out of credit.
    const FLAG_STARVED: u8 = 1;

    fn parts(self) -> (u8, u32) {
        match self {
            Self::Data { len, .. } => (Self::KIND_DATA, len),
            Self::WindowUpdate(value) => (Self::KIND_WINDOW_UPDATE, value),
            Self::ClearLink(value) => (Self::KIND_CLEAR_LINK, value),
            Self::ClearAck(value) => (Self::KIND_CLEAR_ACK, value),
            Self::Ping(value) => (Self::KIND_PING, value),
            Self::Pong(value) => (Self::KIND_PONG, value),
            Self::CleanRtt(value) => (Self::KIND_CLEAN_RTT, value),
        }
    }

    /// Appends this header to `out`.
    pub(crate) fn encode(self, out: &mut BytesMut) {
        let (kind, value) = self.parts();
        out.reserve(HEADER_LEN);
        out.put_u8(kind);
        out.put_u8(match self {
            Self::Data { starved: true, .. } => Self::FLAG_STARVED,
            _ => 0,
        });
        // Padding out to an 8 byte header, so a payload starts aligned.
        out.put_bytes(0, 2);
        out.put_u32(value);
    }

    /// Appends a `DATA` header and the timestamp that belongs with it.
    ///
    /// The timestamp is microseconds on the *sender's* clock, counted from
    /// wherever it started. The receiver never learns that origin and does not
    /// need to: it subtracts a minimum over its own samples, and the unknown
    /// offset between the two clocks cancels out of the difference.
    pub(crate) fn encode_data(len: u32, starved: bool, timestamp: u32, out: &mut BytesMut) {
        Self::Data { len, starved }.encode(out);
        out.put_u32(timestamp);
    }

    /// Reads a header out of exactly [`HEADER_LEN`] bytes.
    pub(crate) fn decode(header: &[u8]) -> io::Result<Self> {
        let kind = header[0];
        let value = u32::from_be_bytes(header[4..HEADER_LEN].try_into().unwrap());
        match kind {
            Self::KIND_DATA if value > MAX_PAYLOAD => Err(io::Error::other(format!(
                "peer sent a {value} byte DATA frame, over the {MAX_PAYLOAD} byte limit"
            ))),
            Self::KIND_DATA => Ok(Self::Data {
                len: value,
                starved: header[1] & Self::FLAG_STARVED != 0,
            }),
            Self::KIND_WINDOW_UPDATE => Ok(Self::WindowUpdate(value)),
            Self::KIND_CLEAR_LINK => Ok(Self::ClearLink(value)),
            Self::KIND_CLEAR_ACK => Ok(Self::ClearAck(value)),
            Self::KIND_PING => Ok(Self::Ping(value)),
            Self::KIND_PONG => Ok(Self::Pong(value)),
            Self::KIND_CLEAN_RTT => Ok(Self::CleanRtt(value)),
            other => Err(io::Error::other(format!("peer sent unknown frame {other}"))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn round_trips_every_frame() {
        let frames = [
            Frame::Data {
                len: MAX_PAYLOAD,
                starved: false,
            },
            Frame::Data {
                len: 1,
                starved: true,
            },
            Frame::WindowUpdate(65536),
            Frame::ClearLink(1),
            Frame::ClearAck(2),
            Frame::Ping(3),
            Frame::Pong(4),
            Frame::CleanRtt(60_000),
        ];
        for frame in frames {
            let mut buf = BytesMut::new();
            frame.encode(&mut buf);
            assert_eq!(buf.len(), HEADER_LEN, "{frame:?} is not one header");
            assert_eq!(Frame::decode(&buf).unwrap(), frame);
        }
    }

    #[test]
    fn rejects_oversized_data_and_unknown_kinds() {
        let mut buf = BytesMut::new();
        Frame::Data {
            len: MAX_PAYLOAD,
            starved: false,
        }
        .encode(&mut buf);
        // Bump the length past the cap without going through the encoder.
        buf[4..].copy_from_slice(&(MAX_PAYLOAD + 1).to_be_bytes());
        assert!(Frame::decode(&buf).is_err());

        buf[0] = 200;
        assert!(Frame::decode(&buf).is_err());
    }
}
