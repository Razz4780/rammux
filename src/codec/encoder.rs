use std::{io, num::NonZeroU16, num::NonZeroU32};

use bytes::Bytes;

use crate::{
    StreamId,
    header::{ControlFlags, Header, MAX_HEADER_LEN, Pong, StreamHeader},
};

/// A frame to be written, before its header is encoded.
///
/// The header is encoded only when the frame starts being written,
/// because it carries the timestamp of its first byte.
#[derive(Debug, Default)]
pub struct OutFrame {
    /// Whether to send a `PING`. Its `t_ping` is set by the encoder.
    pub ping: bool,
    /// A `PONG` to send.
    pub pong: Option<Pong>,
    /// Stream part of the frame.
    pub stream: Option<OutStream>,
    /// Whether this is the `TERM` frame.
    pub term: bool,
}

/// The stream part of an [`OutFrame`]. At least one of the fields must be set.
#[derive(Debug)]
pub struct OutStream {
    pub id: StreamId,
    pub flags: ControlFlags,
    /// Bytes returned to the receive window of the peer, 0 for none.
    pub credit: u32,
    /// Stream data, empty for none. At most `u16::MAX` bytes.
    pub data: Bytes,
}

impl OutFrame {
    /// Returns the `TERM` frame.
    pub fn terminate() -> Self {
        Self {
            term: true,
            ..Default::default()
        }
    }

    /// Returns whether the frame carries nothing, so it must not be sent.
    pub fn is_empty(&self) -> bool {
        !self.ping && self.pong.is_none() && self.stream.is_none() && !self.term
    }

    /// Length of the `DATA` payload in the frame.
    pub fn data_len(&self) -> usize {
        self.stream.as_ref().map_or(0, |stream| stream.data.len())
    }
}

/// An [`OutFrame`] that is being written.
pub struct EncoderItem {
    header: Header,
    payload: Bytes,
    encoded: [u8; MAX_HEADER_LEN],
    header_len: usize,
    /// How many bytes of the frame were accepted by the transport.
    consumed: usize,
}

impl EncoderItem {
    pub fn new(frame: OutFrame) -> Self {
        let (stream, payload) = match frame.stream {
            Some(stream) => {
                let data_len = u16::try_from(stream.data.len()).expect("data too big");
                (
                    Some(StreamHeader {
                        id: stream.id,
                        flags: stream.flags,
                        credit: NonZeroU32::new(stream.credit),
                        data_len: NonZeroU16::new(data_len),
                    }),
                    stream.data,
                )
            },
            None => (None, Bytes::new()),
        };
        let header = Header {
            stream,
            // The value is set when the frame is written.
            ping: frame.ping.then_some(0),
            pong: frame.pong,
            term: frame.term,
        };
        let mut item = Self {
            header,
            payload,
            encoded: [0; MAX_HEADER_LEN],
            header_len: 0,
            consumed: 0,
        };
        item.stamp(0);
        item
    }

    /// Whether the frame carries a `PING`, which needs a stamp.
    pub fn has_ping(&self) -> bool {
        self.header.ping.is_some()
    }

    /// Whether the transport already accepted some bytes of the frame.
    ///
    /// After that, the header is fixed.
    pub fn is_started(&self) -> bool {
        self.consumed > 0
    }

    /// Encodes the header with the given `t_ping`, if `PING` is carried.
    ///
    /// Can be repeated until the frame [starts](Self::is_started) being written.
    pub fn stamp(&mut self, t_ping: u32) {
        assert!(!self.is_started(), "the header is already fixed");
        if self.has_ping() {
            self.header.ping = Some(t_ping);
        }
        self.header_len = self.header.encode(&mut self.encoded);
    }

    /// Returns how many bytes of the frame were not yet accepted by the transport.
    pub fn remaining(&self) -> usize {
        self.header_len + self.payload.len() - self.consumed
    }

    /// Returns the unwritten part of the frame as at most two slices.
    pub fn slices(&self) -> ([io::IoSlice<'_>; 2], usize) {
        let mut slices = [io::IoSlice::new(&[]), io::IoSlice::new(&[])];
        let mut n = 0;
        let header_left = self.header_len.saturating_sub(self.consumed);
        if header_left > 0 {
            slices[n] =
                io::IoSlice::new(&self.encoded[self.header_len - header_left..self.header_len]);
            n += 1;
        }
        let payload_done = self.consumed.saturating_sub(self.header_len);
        if payload_done < self.payload.len() {
            slices[n] = io::IoSlice::new(&self.payload[payload_done..]);
            n += 1;
        }
        (slices, n)
    }

    /// Marks the given number of bytes as accepted by the transport.
    pub fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining(), "overflow");
        self.consumed += cnt;
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::{
        StreamId,
        codec::encoder::{EncoderItem, OutFrame, OutStream},
        header::{ControlFlags, Pong},
    };

    fn collect(item: &EncoderItem) -> Vec<u8> {
        let (slices, n) = item.slices();
        slices[..n].iter().flat_map(|s| s.iter().copied()).collect()
    }

    fn frame() -> OutFrame {
        OutFrame {
            ping: true,
            pong: Some(Pong {
                t_echo: 1,
                t_read: 2,
                rx_bytes: 3,
            }),
            stream: Some(OutStream {
                id: StreamId::from_be_bytes([0, 6]),
                flags: ControlFlags {
                    syn: true,
                    ..Default::default()
                },
                credit: 9,
                data: Bytes::from_static(b"hello"),
            }),
            term: false,
        }
    }

    #[test]
    fn slices_follow_the_progress() {
        let mut item = EncoderItem::new(frame());
        item.stamp(0x0A0B0C0D);
        let all = collect(&item);
        // 2 flags, 2 id, 4 credit, 2 len, 4 t_ping, 12 pong, 5 payload.
        assert_eq!(all.len(), 31);
        assert_eq!(item.remaining(), 31);
        assert_eq!(&all[10..14], &[0x0A, 0x0B, 0x0C, 0x0D]);
        assert_eq!(&all[26..], b"hello");
        assert_eq!(item.slices().1, 2);

        for advanced in 1..=all.len() {
            item.advance(1);
            assert_eq!(collect(&item), &all[advanced..]);
            assert_eq!(item.remaining(), all.len() - advanced);
        }
        assert_eq!(item.slices().1, 0);
    }

    #[test]
    fn restamping_changes_only_the_ping() {
        let mut item = EncoderItem::new(frame());
        item.stamp(77);
        let first = collect(&item);
        item.stamp(78);
        let second = collect(&item);
        assert_eq!(first.len(), second.len());
        assert_eq!(&first[..10], &second[..10]);
        assert_eq!(&second[10..14], &78_u32.to_be_bytes());
        assert_eq!(&first[14..], &second[14..]);
    }

    #[test]
    #[should_panic(expected = "already fixed")]
    fn header_is_fixed_after_the_first_byte() {
        let mut item = EncoderItem::new(frame());
        item.advance(1);
        item.stamp(5);
    }

    #[test]
    fn term_frame() {
        let item = EncoderItem::new(OutFrame::terminate());
        assert_eq!(collect(&item), &[0x01, 0x00]);
    }
}
