use std::{io, num::NonZeroU16, num::NonZeroU32};

use bytes::Bytes;

use crate::{
    StreamId,
    wire::{
        codec::CONTROL_BATCH,
        header::{ControlFlags, Header, MAX_HEADER_LEN, Pong, StreamHeader},
    },
};

/// A frame to be written, before its header is encoded.
///
/// The header is fixed only when the frame starts being written,
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

    /// Returns the header of the frame, with `t_ping` 0 if `PING` is carried, and the payload.
    pub fn into_parts(self) -> (Header, Bytes) {
        let (stream, payload) = match self.stream {
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
            ping: self.ping.then_some(0),
            pong: self.pong,
            term: self.term,
        };
        (header, payload)
    }
}

/// A frame with a payload that is being written.
pub struct EncoderItem {
    header: Header,
    payload: Bytes,
    encoded: [u8; MAX_HEADER_LEN],
    header_len: usize,
    /// How many bytes of the frame were accepted by the transport.
    consumed: usize,
}

impl EncoderItem {
    pub fn new(header: Header, payload: Bytes) -> Self {
        let mut item = Self {
            header,
            payload,
            encoded: [0; MAX_HEADER_LEN],
            header_len: 0,
            consumed: 0,
        };
        item.header_len = item.header.encode(&mut item.encoded);
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

/// Frames without payload, encoded back to back, so that they are written together.
///
/// Holds at most [`CONTROL_BATCH`] bytes.
pub struct ControlBuffer {
    bytes: Vec<u8>,
    /// The frames that carry a `PING`, in order: where each starts in `bytes`, and its header.
    pings: Vec<(usize, Header)>,
    /// How many bytes were accepted by the transport.
    consumed: usize,
}

impl ControlBuffer {
    pub fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(CONTROL_BATCH),
            pings: Vec::new(),
            consumed: 0,
        }
    }

    /// Returns whether another frame fits.
    ///
    /// The space is freed only when the transport accepted all bytes.
    pub fn has_room(&self) -> bool {
        self.bytes.len() + MAX_HEADER_LEN <= CONTROL_BATCH
    }

    /// Encodes the frame with the given header at the end of the buffer.
    ///
    /// The frame must have no payload, and the buffer must [have room](Self::has_room).
    pub fn push(&mut self, header: Header) {
        assert!(self.has_room(), "control buffer is full");
        assert!(
            header.stream.is_none_or(|stream| stream.data_len.is_none()),
            "frame with payload"
        );
        let mut encoded = [0; MAX_HEADER_LEN];
        let len = header.encode(&mut encoded);
        if header.ping.is_some() {
            self.pings.push((self.bytes.len(), header));
        }
        self.bytes.extend_from_slice(&encoded[..len]);
    }

    /// Returns whether a frame with a `PING` was not started yet, so it needs a stamp.
    pub fn needs_stamp(&self) -> bool {
        // The last one is started last.
        self.pings
            .last()
            .is_some_and(|(start, _)| *start >= self.consumed)
    }

    /// Encodes again, with the given `t_ping`, the frames with a `PING` of which the transport accepted nothing.
    pub fn stamp(&mut self, t_ping: u32) {
        let consumed = self.consumed;
        for (start, header) in &mut self.pings {
            if *start < consumed {
                continue;
            }
            header.ping = Some(t_ping);
            let mut encoded = [0; MAX_HEADER_LEN];
            let len = header.encode(&mut encoded);
            self.bytes[*start..*start + len].copy_from_slice(&encoded[..len]);
        }
    }

    /// Returns the bytes not yet accepted by the transport.
    pub fn unwritten(&self) -> &[u8] {
        &self.bytes[self.consumed..]
    }

    /// Returns whether all frames were accepted by the transport.
    pub fn is_empty(&self) -> bool {
        self.unwritten().is_empty()
    }

    /// Marks the given number of bytes as accepted by the transport.
    pub fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.unwritten().len(), "overflow");
        self.consumed += cnt;
        if self.consumed == self.bytes.len() {
            self.bytes.clear();
            self.pings.clear();
            self.consumed = 0;
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::{
        StreamId,
        wire::{
            codec::{
                CONTROL_BATCH,
                encoder::{ControlBuffer, EncoderItem, OutFrame, OutStream},
            },
            header::{ControlFlags, Header, MAX_HEADER_LEN, Pong},
        },
    };

    fn collect(item: &EncoderItem) -> Vec<u8> {
        let (slices, n) = item.slices();
        slices[..n].iter().flat_map(|s| s.iter().copied()).collect()
    }

    fn frame() -> OutFrame {
        OutFrame {
            ping: true,
            pong: Some(Pong { t_echo: 1 }),
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

    fn item() -> EncoderItem {
        let (header, payload) = frame().into_parts();
        EncoderItem::new(header, payload)
    }

    #[test]
    fn slices_follow_the_progress() {
        let mut item = item();
        item.stamp(0x0A0B0C0D);
        let all = collect(&item);
        // 2 flags, 2 id, 4 credit, 2 len, 4 t_ping, 4 pong, 5 payload.
        assert_eq!(all.len(), 23);
        assert_eq!(item.remaining(), 23);
        assert_eq!(&all[10..14], &[0x0A, 0x0B, 0x0C, 0x0D]);
        assert_eq!(&all[18..], b"hello");
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
        let mut item = item();
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
        let mut item = item();
        item.advance(1);
        item.stamp(5);
    }

    fn ping() -> Header {
        Header {
            ping: Some(0),
            ..Default::default()
        }
    }

    fn pong() -> Header {
        Header {
            pong: Some(Pong { t_echo: 0x0A0B0C0D }),
            ..Default::default()
        }
    }

    fn term() -> Header {
        OutFrame::terminate().into_parts().0
    }

    #[test]
    fn control_frames_are_encoded_back_to_back() {
        let mut buffer = ControlBuffer::new();
        buffer.push(pong());
        buffer.push(term());
        assert_eq!(
            buffer.unwritten(),
            &[0x00, 0x40, 0x0A, 0x0B, 0x0C, 0x0D, 0x00, 0x80]
        );
        assert!(!buffer.needs_stamp());
    }

    #[test]
    fn stamping_skips_the_started_frames() {
        let mut buffer = ControlBuffer::new();
        buffer.push(ping());
        buffer.push(pong());
        buffer.push(ping());
        buffer.stamp(1);
        assert_eq!(
            buffer.unwritten(),
            &[
                0x00, 0x20, 0, 0, 0, 1, // PING
                0x00, 0x40, 0x0A, 0x0B, 0x0C, 0x0D, // PONG
                0x00, 0x20, 0, 0, 0, 1, // PING
            ]
        );

        // The first PING is started, the second is not.
        buffer.advance(1);
        assert!(buffer.needs_stamp());
        buffer.stamp(2);
        assert_eq!(&buffer.unwritten()[..5], &[0x20, 0, 0, 0, 1]);
        assert_eq!(&buffer.unwritten()[11..], &[0x00, 0x20, 0, 0, 0, 2]);

        // Both are started.
        buffer.advance(12);
        assert!(!buffer.needs_stamp());
        buffer.stamp(3);
        assert_eq!(buffer.unwritten(), &[0x20, 0, 0, 0, 2]);
    }

    #[test]
    fn room_is_freed_when_everything_is_written() {
        let mut buffer = ControlBuffer::new();
        while buffer.has_room() {
            buffer.push(pong());
        }
        let len = buffer.unwritten().len();
        assert!(len <= CONTROL_BATCH && len + MAX_HEADER_LEN > CONTROL_BATCH);

        buffer.advance(len - 1);
        assert!(!buffer.has_room());
        buffer.advance(1);
        assert!(buffer.is_empty());
        assert!(buffer.has_room());
        buffer.push(ping());
        assert!(buffer.needs_stamp());
        assert_eq!(buffer.unwritten(), &[0x00, 0x20, 0, 0, 0, 0]);
    }

    #[test]
    #[should_panic(expected = "frame with payload")]
    fn control_buffer_takes_no_payload() {
        let (header, _) = frame().into_parts();
        ControlBuffer::new().push(header);
    }
}
