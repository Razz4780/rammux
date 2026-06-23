use std::io;

use bytes::{Buf, Bytes};

use crate::{
    StreamId,
    header::{ControlFlags, PingPayload, RawFlags, RawHeader},
};

pub struct EncoderItem {
    headers: [u8; RawHeader::LEN * 2],
    data: Bytes,
    consumed: usize,
}

impl EncoderItem {
    pub fn new_ping(payload: PingPayload, is_response: bool) -> Self {
        let flags = if is_response {
            RawFlags::PING
        } else {
            RawFlags::PING | RawFlags::SYN
        };
        let header = RawHeader {
            stream_id: payload.stream_id,
            flags,
            len: payload.len,
        };
        let mut headers = [0_u8; RawHeader::LEN * 2];
        headers[RawHeader::LEN..].copy_from_slice(&header.encode());
        Self {
            headers,
            data: Default::default(),
            consumed: RawHeader::LEN,
        }
    }

    pub fn new_terminate() -> Self {
        Self {
            headers: [0_u8; RawHeader::LEN * 2],
            data: Default::default(),
            consumed: RawHeader::LEN,
        }
    }

    pub fn new_window_update(stream_id: StreamId, flags: ControlFlags, update: u32) -> Self {
        let header = RawHeader {
            stream_id,
            flags: RawFlags::from(flags).union(RawFlags::WINDOW_UPDATE),
            len: update,
        };
        let mut headers = [0; RawHeader::LEN * 2];
        headers[8..].copy_from_slice(&header.encode());
        Self {
            headers,
            data: Default::default(),
            consumed: RawHeader::LEN,
        }
    }

    pub fn new_data(stream_id: StreamId, flags: ControlFlags, data: Bytes) -> Self {
        let header = RawHeader {
            stream_id,
            flags: RawFlags::from(flags).union(RawFlags::DATA),
            len: u32::try_from(data.len()).expect("data too big"),
        };
        let mut headers = [0; RawHeader::LEN * 2];
        headers[8..].copy_from_slice(&header.encode());
        Self {
            headers,
            data,
            consumed: RawHeader::LEN,
        }
    }

    pub fn new_window_update_and_data(
        stream_id: StreamId,
        flags: ControlFlags,
        update: u32,
        data: Bytes,
    ) -> Self {
        let flags = RawFlags::from(flags);
        let header_1 = RawHeader {
            stream_id,
            flags: flags
                .union(RawFlags::WINDOW_UPDATE)
                .difference(RawFlags::FIN_WRITE),
            len: update,
        };
        let header_2 = RawHeader {
            stream_id,
            flags: flags
                .union(RawFlags::DATA)
                .difference(RawFlags::FIN_READ | RawFlags::SYN),
            len: u32::try_from(data.len()).expect("data too big"),
        };
        let mut headers = [0; RawHeader::LEN * 2];
        headers[..8].copy_from_slice(&header_1.encode());
        headers[8..].copy_from_slice(&header_2.encode());
        Self {
            headers,
            data,
            consumed: 0,
        }
    }
}

impl Buf for EncoderItem {
    fn remaining(&self) -> usize {
        self.headers.len() + self.data.len() - self.consumed
    }

    fn chunk(&self) -> &[u8] {
        match self.consumed.checked_sub(self.headers.len()) {
            Some(offset) => &self.data[offset..],
            None => &self.headers[self.consumed..],
        }
    }

    fn advance(&mut self, cnt: usize) {
        if self.remaining() < cnt {
            panic!("overflow")
        }
        self.consumed += cnt;
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        let mut idx = 0;
        let mut offset = self.consumed;
        for chunk in [self.headers.as_slice(), self.data.as_ref()] {
            let data = chunk.get(offset..).unwrap_or_default();
            offset = offset.saturating_sub(chunk.len());
            if data.is_empty() {
                continue;
            }
            let Some(slot) = dst.get_mut(idx) else {
                break;
            };
            *slot = io::IoSlice::new(data);
            idx += 1;
        }
        idx
    }
}

#[cfg(test)]
mod test {
    use std::{io, ops::Not};

    use bytes::{Buf, Bytes};
    use rstest::rstest;

    use crate::{
        StreamId,
        codec::EncoderItem,
        header::{ControlFlags, PingPayload, RawHeader},
    };

    #[rstest]
    #[case::request(
        PingPayload {
            stream_id: StreamId::from_be_bytes([1, 2, 3]),
            len: 1337,
        },
        false,
        &[1, 2, 3, 33, 0, 0, 5, 57],
    )]
    #[case::response(
        PingPayload {
            stream_id: StreamId::from_be_bytes([1, 2, 3]),
            len: 1337,
        },
        true,
        &[1, 2, 3, 1, 0, 0, 5, 57],
    )]
    #[test]
    fn ping_frame(
        #[case] payload: PingPayload,
        #[case] is_response: bool,
        #[case] mut expected: &[u8],
    ) {
        let mut item = EncoderItem::new_ping(payload, is_response);

        assert_eq!(item.remaining(), expected.len());
        assert_eq!(item.chunk(), expected);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(1, item.chunks_vectored(&mut slices));
        assert_eq!(slices[0].as_ref(), expected);

        expected = expected.split_at(RawHeader::LEN / 2).1;
        item.advance(RawHeader::LEN / 2);
        assert_eq!(item.remaining(), RawHeader::LEN / 2);
        assert_eq!(item.chunk(), expected);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(1, item.chunks_vectored(&mut slices));
        assert_eq!(slices[0].as_ref(), expected);

        item.advance(RawHeader::LEN / 2);
        assert_eq!(item.remaining(), 0);
        assert_eq!(item.chunk(), &[]);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(0, item.chunks_vectored(&mut slices));
    }

    #[test]
    fn term_frame() {
        let mut expected = [0; RawHeader::LEN].as_slice();
        let mut item = EncoderItem::new_terminate();

        assert_eq!(item.remaining(), expected.len());
        assert_eq!(item.chunk(), expected);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(1, item.chunks_vectored(&mut slices));
        assert_eq!(slices[0].as_ref(), expected);

        expected = expected.split_at(RawHeader::LEN / 2).1;
        item.advance(RawHeader::LEN / 2);
        assert_eq!(item.remaining(), RawHeader::LEN / 2);
        assert_eq!(item.chunk(), expected);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(1, item.chunks_vectored(&mut slices));
        assert_eq!(slices[0].as_ref(), expected);

        item.advance(RawHeader::LEN / 2);
        assert_eq!(item.remaining(), 0);
        assert_eq!(item.chunk(), &[]);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(0, item.chunks_vectored(&mut slices));
    }

    #[rstest]
    #[case::window_update(
        EncoderItem::new_window_update(
            StreamId::from_be_bytes([4, 3, 2]),
            ControlFlags {
                syn: true,
                fin_read: true,
                fin_write: false,
            },
            2137,
        ),
        &[4, 3, 2, 42, 0, 0, 8, 89],
    )]
    #[case::data(
        EncoderItem::new_data(
            StreamId::from_be_bytes([71, 99, 21]),
            ControlFlags {
                syn: false,
                fin_read: false,
                fin_write: true,
            },
            Bytes::from_static(b"9999"),
        ),
        &[71, 99, 21, 20, 0, 0, 0, 4, 57, 57, 57, 57],
    )]
    #[case::both(
        EncoderItem::new_window_update_and_data(
            StreamId::from_be_bytes([7, 6, 5]),
            ControlFlags {
                syn: true,
                fin_read: true,
                fin_write: true,
            },
            12,
            Bytes::from_static(b"2137"),
        ),
        &[
            7, 6, 5, 42, 0, 0, 0, 12,
            7, 6, 5, 20, 0, 0, 0, 4, 50, 49, 51, 55,
        ],
    )]
    #[test]
    fn stream_update_frame(#[case] mut item: EncoderItem, #[case] mut expected: &[u8]) {
        while expected.is_empty().not() {
            let remaining = item.remaining();
            assert_eq!(remaining, expected.len());
            let chunk = item.chunk();
            assert!(chunk.is_empty().not());
            assert!(expected.starts_with(chunk));

            let mut slices_arr = [io::IoSlice::new(&[]); 4];
            let filled = item.chunks_vectored(&mut slices_arr);
            assert!(filled > 0);
            let mut slices = slices_arr.get_mut(..filled).unwrap();
            let total_len = slices.iter().map(|slice| slice.len()).sum::<usize>();
            assert_eq!(total_len, expected.len());
            let mut expected_suffix = expected;
            while expected_suffix.is_empty().not() {
                assert!(slices[0].is_empty().not());
                assert!(expected_suffix.starts_with(slices[0].as_ref()));
                expected_suffix = expected_suffix.split_at(slices[0].len()).1;
                slices = slices.split_at_mut(1).1;
            }

            expected = expected.split_at(1).1;
            item.advance(1);
        }

        assert_eq!(item.remaining(), 0);
        assert_eq!(item.chunk(), &[]);
        let mut slices = [io::IoSlice::new(&[]); 4];
        assert_eq!(item.chunks_vectored(&mut slices), 0);
    }
}
