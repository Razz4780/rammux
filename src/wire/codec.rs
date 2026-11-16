use std::{
    io::{self, IoSlice},
    num::NonZeroU16,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{FutureExt, Sink, Stream};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};

use crate::{
    buffer::Buffer,
    error::{DecodeError, ErrorKind},
    wire::{
        clock::Clock,
        codec::{
            decoder::{DecodedFrame, DecoderState, InboundFrame},
            encoder::{ControlBuffer, EncoderItem, OutFrame},
        },
        header::{Flags, Header},
    },
};

pub mod decoder;
pub mod encoder;

/// Most bytes of frames without payload that the codec holds before it writes them.
pub const CONTROL_BATCH: usize = 1024;

/// rammux frame codec applied to an IO transport.
///
/// Implements [`Stream`] and [`Sink`].
///
/// # Frame validation
///
/// This codec implements only basic frame validation, as it does not see the state of the whole connection.
/// Yielded [`DecodedFrame`]s can have arbitrary values, except that the payload
/// is never empty and never larger than the configured frame limit,
/// `WINDOW` credit is never 0, and the flags make sense (see `PROTOCOL.md`).
///
/// # Encoder
///
/// Frames without payload are encoded into a control buffer of at most [`CONTROL_BATCH`] bytes.
/// A frame with a payload waits on its own, and is written in one `poll_write_vectored`
/// together with the control buffer before it (the control frames, the header, the payload).
/// The control buffer is written alone when it has no room for another frame, on a flush or a close,
/// and by `poll_ready` after a flush or a close whose writes did not finish.
/// Either way the bytes reach the transport in the order the frames were sent.
///
/// The codec is ready for the next frame when no frame with a payload waits and the control buffer has room.
/// After a flush or a close whose writes did not finish, it is ready only when everything is written.
/// There is no queue beyond that, so the caller decides what goes next.
///
/// The header of a frame with a `PING` is encoded again before each write, until the transport
/// accepts a byte of the frame, so that the `PING` timestamp is the time of the first accepted byte.
///
/// A transport without native vectored writes (`is_write_vectored()` is false) gets tokio's default
/// `poll_write_vectored`, which writes the first non-empty slice only: the control frames, the header,
/// and the payload each take a write.
/// Such a transport is best wrapped in a buffered writer (e.g. `tokio::io::BufWriter`), which this codec flushes
/// after the last frame it has ready. A buffered reader is fine: what it has buffered stays in the transport
/// returned by the downgrade.
pub struct RammuxCodec<IO> {
    io: IO,
    /// Frames without payload, not yet written.
    control: ControlBuffer,
    /// The frame with a payload that waits behind `control`.
    pending: Option<EncoderItem>,
    /// Whether a flush or a close did not finish writing the frames.
    flush_unfinished: bool,
    decoder: DecoderState,
    frame_limit: NonZeroU16,
    clock: Clock,
    inbound_frame_sleep: Pin<Box<Sleep>>,
    inbound_frame_timeout: Duration,
}

impl<IO> RammuxCodec<IO> {
    pub fn new(io: IO, frame_limit: NonZeroU16, inbound_frame_timeout: Duration) -> Self {
        Self {
            io,
            control: ControlBuffer::new(),
            pending: None,
            flush_unfinished: false,
            decoder: Default::default(),
            frame_limit,
            clock: Clock::new(),
            inbound_frame_sleep: Box::pin(tokio::time::sleep_until(crate::deadline(
                inbound_frame_timeout,
            ))),
            inbound_frame_timeout,
        }
    }

    /// Returns the private clock of this side, which stamps the `PING`s.
    pub fn clock(&self) -> Clock {
        self.clock
    }

    /// Returns the transport. Everything sent must have been written by a flush or a close.
    pub fn into_inner(self) -> IO {
        debug_assert!(
            self.control.is_empty() && self.pending.is_none(),
            "frames left unwritten"
        );
        self.io
    }
}

impl<IO> RammuxCodec<IO>
where
    IO: AsyncRead + Unpin,
{
    fn poll_next_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<DecodedFrame, ErrorKind>>> {
        loop {
            match &mut self.decoder {
                DecoderState::ReadingHeader {
                    filled,
                    need,
                    flags,
                    buffer,
                } => {
                    let mut buf = ReadBuf::new(&mut buffer[*filled..*need]);
                    std::task::ready!(Pin::new(&mut self.io).poll_read(cx, &mut buf))?;
                    match buf.filled().len() {
                        0 if *filled == 0 => break Poll::Ready(None),
                        0 => break Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into()))),
                        n => *filled += n,
                    }
                    if *filled < *need {
                        continue;
                    }

                    let valid_flags = match flags {
                        Some(flags) => *flags,
                        None => {
                            // Only the flags are read, now we know how much more to read.
                            let valid =
                                Flags::validate(u16::from_be_bytes([buffer[0], buffer[1]]))?;
                            *need = valid.header_len();
                            *flags = Some(valid);
                            if *filled < *need {
                                continue;
                            }
                            valid
                        },
                    };
                    let header_at = Instant::now();
                    let header = Header::decode(valid_flags, &buffer[..*need])?;

                    if header.term {
                        self.decoder = Default::default();
                        break Poll::Ready(Some(Ok(DecodedFrame::Terminate)));
                    }
                    let Some(len) = header.stream.and_then(|stream| stream.data_len) else {
                        self.decoder = Default::default();
                        break Poll::Ready(Some(Ok(DecodedFrame::Frame(InboundFrame {
                            header,
                            data: Default::default(),
                            header_at,
                        }))));
                    };
                    if len > self.frame_limit {
                        break Poll::Ready(Some(Err(DecodeError(
                            "DATA exceeding the configured frame limit",
                        )
                        .into())));
                    }
                    self.decoder = DecoderState::ReadingData {
                        header,
                        header_at,
                        filled: 0,
                        buffer: Buffer::with_capacity(usize::from(len.get())),
                    };
                },

                DecoderState::ReadingData {
                    header,
                    header_at,
                    buffer,
                    filled,
                } => {
                    let unfilled = &mut buffer.as_mut_slice()[*filled..];
                    let mut read_buf = ReadBuf::uninit(unfilled);

                    std::task::ready!(Pin::new(&mut self.io).poll_read(cx, &mut read_buf))?;
                    let read = read_buf.filled().len();

                    if read == 0 {
                        break Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())));
                    }

                    *filled += read;
                    if *filled == buffer.as_slice().len() {
                        // SAFETY: the whole buffer was filled by the reads.
                        let data = unsafe { std::mem::take(buffer).assume_init() };
                        let output = DecodedFrame::Frame(InboundFrame {
                            header: *header,
                            data,
                            header_at: *header_at,
                        });
                        self.decoder = Default::default();
                        break Poll::Ready(Some(Ok(output)));
                    }
                },
            }
        }
    }
}

impl<IO> Stream for RammuxCodec<IO>
where
    IO: AsyncRead + Unpin,
{
    type Item = Result<DecodedFrame, ErrorKind>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.poll_next_frame(cx) {
            Poll::Pending => {
                std::task::ready!(this.inbound_frame_sleep.poll_unpin(cx));
                Poll::Ready(Some(Err(ErrorKind::Io(io::ErrorKind::TimedOut.into()))))
            },
            Poll::Ready(result) => {
                this.inbound_frame_sleep
                    .as_mut()
                    .reset(crate::deadline(this.inbound_frame_timeout));
                Poll::Ready(result)
            },
        }
    }
}

impl<IO> RammuxCodec<IO>
where
    IO: AsyncWrite + Unpin,
{
    /// Writes the control buffer and the waiting frame (if any), until the next frame can be sent
    /// (`all` is false) or everything is written (`all` is true).
    fn poll_write(&mut self, cx: &mut Context<'_>, all: bool) -> Poll<Result<(), ErrorKind>> {
        loop {
            if let Some(item) = &self.pending
                && item.remaining() == 0
            {
                self.pending = None;
            }
            let done = self.pending.is_none()
                && if all {
                    self.control.is_empty()
                } else {
                    self.control.has_room()
                };
            if done {
                if all {
                    self.flush_unfinished = false;
                }
                break Poll::Ready(Ok(()));
            }

            // Until the transport accepts a byte of a frame, its timestamp is whatever the time is now.
            let pending_needs_stamp = self
                .pending
                .as_ref()
                .is_some_and(|item| item.has_ping() && !item.is_started());
            if pending_needs_stamp || self.control.needs_stamp() {
                let now = self.clock.micros(Instant::now());
                self.control.stamp(now);
                if let Some(item) = self.pending.as_mut().filter(|_| pending_needs_stamp) {
                    item.stamp(now);
                }
            }

            // The control frames, the header and the payload go to the transport together.
            // A transport without vectored writes gets them one after another, which is the best it can do.
            let control = self.control.unwritten();
            let control_len = control.len();
            let mut slices = [IoSlice::new(&[]); 3];
            let mut n_slices = 0;
            if control_len > 0 {
                slices[0] = IoSlice::new(control);
                n_slices = 1;
            }
            if let Some(item) = &self.pending {
                let (item_slices, n_item) = item.slices();
                slices[n_slices..n_slices + n_item].copy_from_slice(&item_slices[..n_item]);
                n_slices += n_item;
            }
            let written = std::task::ready!(
                Pin::new(&mut self.io).poll_write_vectored(cx, &slices[..n_slices])
            )?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }

            // The control frames go first, so what the transport accepted of them
            // does not start the waiting frame.
            match &mut self.pending {
                Some(item) => {
                    let to_control = written.min(control_len);
                    self.control.advance(to_control);
                    item.advance(written - to_control);
                },
                None => self.control.advance(written),
            }
        }
    }
}

impl<IO> Sink<OutFrame> for RammuxCodec<IO>
where
    IO: AsyncWrite + Unpin,
{
    type Error = ErrorKind;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        // A flush that did not finish means the transport takes no more. Until it does, the next frames
        // wait with the caller rather than in the control buffer, where they would go stale: the caller
        // replaces an owed `PONG` with a newer one, and a window update grows with what is read meanwhile.
        let all = this.flush_unfinished;
        this.poll_write(cx, all)
    }

    fn start_send(self: Pin<&mut Self>, frame: OutFrame) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.pending.is_some() {
            panic!("frame encoder is busy");
        }
        assert!(!frame.is_empty(), "empty frame");
        let (header, payload) = frame.into_parts();
        if payload.is_empty() {
            this.control.push(header);
        } else {
            this.pending = Some(EncoderItem::new(header, payload));
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.flush_unfinished = true;
        std::task::ready!(this.poll_write(cx, true))?;
        Pin::new(&mut this.io)
            .poll_flush(cx)
            .map_err(ErrorKind::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.flush_unfinished = true;
        std::task::ready!(this.poll_write(cx, true))?;
        Pin::new(&mut this.io)
            .poll_shutdown(cx)
            .map_err(ErrorKind::from)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        num::{NonZeroU16, NonZeroU32},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use bytes::Bytes;
    use futures::{FutureExt, SinkExt, StreamExt};
    use rstest::rstest;
    use tokio::{
        io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
        time::Instant,
    };

    use crate::{
        StreamId,
        error::ErrorKind,
        wire::{
            codec::{
                CONTROL_BATCH, RammuxCodec,
                decoder::DecodedFrame,
                encoder::{OutFrame, OutStream},
            },
            header::{ControlFlags, Pong},
        },
    };

    const LIMIT: NonZeroU16 = NonZeroU16::new(11).unwrap();

    fn codec<IO>(io: IO) -> RammuxCodec<IO> {
        RammuxCodec::new(io, LIMIT, Duration::from_secs(1))
    }

    fn stream_frame(id: u16, flags: ControlFlags, credit: u32, data: &'static [u8]) -> OutFrame {
        OutFrame {
            stream: Some(OutStream {
                id: StreamId::from_be_bytes(id.to_be_bytes()),
                flags,
                credit,
                data: Bytes::from_static(data),
            }),
            ..Default::default()
        }
    }

    fn syn() -> ControlFlags {
        ControlFlags {
            syn: true,
            ..Default::default()
        }
    }

    type Writes = Arc<Mutex<Vec<Vec<u8>>>>;

    /// Records every call to write, and can be told to refuse the next one.
    struct MockIo {
        writes: Writes,
        vectored: bool,
        refuse: Arc<Mutex<bool>>,
        max_accept: usize,
        /// Refuses every write that follows an accepted one, so that each poll writes once.
        one_write_per_poll: bool,
        accepted: bool,
    }

    impl MockIo {
        fn new(vectored: bool) -> (Self, Writes, Arc<Mutex<bool>>) {
            let writes = Arc::new(Mutex::new(Vec::new()));
            let refuse = Arc::new(Mutex::new(false));
            let io = Self {
                writes: writes.clone(),
                vectored,
                refuse: refuse.clone(),
                max_accept: usize::MAX,
                one_write_per_poll: false,
                accepted: false,
            };
            (io, writes, refuse)
        }

        fn write(&mut self, cx: &mut Context<'_>, bufs: &[&[u8]]) -> Poll<io::Result<usize>> {
            let refuse = std::mem::take(&mut *self.refuse.lock().unwrap())
                || (self.one_write_per_poll && self.accepted);
            self.accepted = !refuse;
            if refuse {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let mut left = self.max_accept;
            let mut out = Vec::new();
            for buf in bufs {
                let take = buf.len().min(left);
                out.extend_from_slice(&buf[..take]);
                left -= take;
            }
            let n = out.len();
            self.writes.lock().unwrap().push(out);
            Poll::Ready(Ok(n))
        }
    }

    impl AsyncWrite for MockIo {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().write(cx, &[buf])
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            if !self.vectored {
                // Same as the default: the first non-empty slice.
                let first = bufs
                    .iter()
                    .find(|b| !b.is_empty())
                    .map_or(&[][..], |b| &**b);
                return self.get_mut().write(cx, &[first]);
            }
            let slices = bufs.iter().map(|b| &**b).collect::<Vec<_>>();
            self.get_mut().write(cx, &slices)
        }

        fn is_write_vectored(&self) -> bool {
            self.vectored
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for MockIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn encode_decode() {
        let frames = || {
            vec![
                OutFrame {
                    ping: true,
                    ..Default::default()
                },
                OutFrame {
                    pong: Some(Pong { t_echo: 1 }),
                    ..Default::default()
                },
                stream_frame(0x0101, syn(), 2137, b""),
                stream_frame(
                    0x0202,
                    ControlFlags {
                        fin_read: true,
                        ..Default::default()
                    },
                    0,
                    b"hello",
                ),
                stream_frame(
                    0x0303,
                    ControlFlags {
                        fin_write: true,
                        ..Default::default()
                    },
                    0,
                    b"",
                ),
                OutFrame {
                    ping: true,
                    pong: Some(Pong { t_echo: 9 }),
                    ..stream_frame(
                        0x0404,
                        ControlFlags {
                            syn: true,
                            fin_read: true,
                            fin_write: true,
                            abort: true,
                        },
                        5,
                        b"eleven byte",
                    )
                },
                OutFrame::terminate(),
            ]
        };

        let (io_1, io_2) = tokio::io::duplex(8);
        let mut codec_1 = codec(io_1);
        let mut codec_2 = codec(io_2);

        tokio::join!(
            async {
                for frame in frames() {
                    codec_1.feed(frame).await.unwrap();
                }
                codec_1.close().await.unwrap();
            },
            async {
                for sent in frames() {
                    let received = codec_2.next().await.unwrap().unwrap();
                    assert_same(&sent, &received);
                }
                assert!(codec_2.next().await.is_none());
            },
        );
    }

    /// Checks that the received frame is the sent one (except for `t_ping`, which the encoder sets).
    fn assert_same(sent: &OutFrame, received: &DecodedFrame) {
        let DecodedFrame::Frame(received) = received else {
            assert!(sent.term, "unexpected TERM instead of {sent:?}");
            return;
        };
        assert!(!sent.term, "expected TERM, got {received:?}");
        assert_eq!(received.header.ping.is_some(), sent.ping);
        assert_eq!(received.header.pong, sent.pong);
        match &sent.stream {
            None => assert!(received.header.stream.is_none()),
            Some(sent) => {
                let stream = received.header.stream.unwrap();
                assert_eq!(stream.id, sent.id);
                assert_eq!(stream.flags, sent.flags);
                assert_eq!(stream.credit.map_or(0, NonZeroU32::get), sent.credit);
                assert_eq!(received.data.as_ref(), sent.data.as_ref());
            },
        }
    }

    /// Checks that the received frames are the sent ones, in order.
    fn assert_all_same(sent: &[OutFrame], received: &[DecodedFrame]) {
        assert_eq!(sent.len(), received.len());
        for (sent, received) in sent.iter().zip(received) {
            assert_same(sent, received);
        }
    }

    /// Feeds raw bytes to a codec and returns the first result.
    async fn decode(bytes: &[u8]) -> Option<Result<DecodedFrame, ErrorKind>> {
        let (mut writer, reader) = tokio::io::duplex(256);
        writer.write_all(bytes).await.unwrap();
        writer.shutdown().await.unwrap();
        codec(reader).next().await
    }

    #[rstest]
    #[case::reserved_bits(&[0x04, 0x10, 0, 1, 0, 1, b'a'])]
    #[case::reserved_bit_0x0200(&[0x02, 0x10, 0, 1, 0, 1, b'a'])]
    #[case::abort_without_fin_write(&[0x01, 0x10, 0, 1, 0, 1, b'a'])]
    #[case::high_reserved_bits(&[0x80, 0x10, 0, 1, 0, 1, b'a'])]
    #[case::no_flags(&[0x00, 0x00])]
    #[case::term_and_ping(&[0x00, 0xA0, 0, 0, 0, 1])]
    #[case::term_and_stream(&[0x00, 0x84, 0, 1])]
    #[case::data_len_zero(&[0x00, 0x10, 0, 1, 0, 0])]
    #[case::data_len_over_limit(&[0x00, 0x10, 0, 1, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]
    #[case::window_credit_zero(&[0x00, 0x08, 0, 1, 0, 0, 0, 0])]
    #[tokio::test]
    async fn framing_errors(#[case] bytes: &[u8]) {
        decode(bytes).await.unwrap().unwrap_err();
    }

    #[rstest]
    #[case::truncated_flags(&[0x00])]
    #[case::truncated_header(&[0x00, 0x10, 0, 1, 0])]
    #[case::truncated_payload(&[0x00, 0x10, 0, 1, 0, 4, 1, 2])]
    #[tokio::test]
    async fn truncated(#[case] bytes: &[u8]) {
        let error = decode(bytes).await.unwrap().unwrap_err();
        assert!(matches!(error, ErrorKind::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof));
    }

    #[tokio::test]
    async fn clean_eof() {
        assert!(decode(&[]).await.is_none());
    }

    #[tokio::test]
    async fn decodes_the_layout_of_the_spec() {
        let bytes = [
            0x00, 0x7B, // SYN | FIN_READ | WINDOW | DATA | PING | PONG
            0x12, 0x34, // stream_id
            0x11, 0x22, 0x33, 0x44, // credit
            0x00, 0x02, // len
            0x99, 0xAA, 0xBB, 0xCC, // t_ping
            0x0A, 0x0B, 0x0C, 0x0D, // t_echo
            b'o', b'k',
        ];
        let Some(Ok(DecodedFrame::Frame(frame))) = decode(&bytes).await else {
            panic!("expected a frame");
        };
        assert_eq!(frame.header.ping, Some(0x99AABBCC));
        assert_eq!(frame.header.pong, Some(Pong { t_echo: 0x0A0B0C0D }));
        let stream = frame.header.stream.unwrap();
        assert_eq!(stream.id, StreamId::from_be_bytes([0x12, 0x34]));
        assert!(stream.flags.syn && stream.flags.fin_read && !stream.flags.fin_write);
        assert_eq!(stream.credit.unwrap().get(), 0x11223344);
        assert_eq!(frame.data.as_ref(), b"ok");
    }

    /// Decodes the frames of a byte buffer.
    async fn decode_all(bytes: &[u8]) -> Vec<DecodedFrame> {
        let mut codec = codec(bytes);
        let mut frames = Vec::new();
        while let Some(frame) = codec.next().await {
            frames.push(frame.unwrap());
        }
        frames
    }

    /// Returns the bytes of the given frame, as the encoder writes them at the given time.
    async fn encode(frame: OutFrame, vectored: bool) -> Vec<Vec<u8>> {
        let (io, writes, _) = MockIo::new(vectored);
        let mut codec = codec(io);
        codec.send(frame).await.unwrap();
        let writes = writes.lock().unwrap();
        writes.clone()
    }

    #[tokio::test]
    async fn one_write_per_frame() {
        let frame = OutFrame {
            ping: true,
            ..stream_frame(7, syn(), 100, b"payload")
        };
        let writes = encode(frame, true).await;
        assert_eq!(writes.len(), 1, "header and payload are written together");
        let frames = decode_all(&writes[0]).await;
        assert_eq!(frames.len(), 1);
    }

    #[tokio::test]
    async fn a_transport_without_vectored_writes_gets_the_header_first() {
        let frame = OutFrame {
            ping: true,
            ..stream_frame(7, syn(), 100, b"payload")
        };
        let writes = encode(frame, false).await;
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[1], b"payload");
        let frames = decode_all(&writes.concat()).await;
        assert_eq!(frames.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn partial_writes_resume_in_place(
        #[values(true, false)] vectored: bool,
        // Inside the header, at the end of the header, inside the payload.
        #[values(3, 14, 16, 20)] max_accept: usize,
    ) {
        let (mut io, writes, _) = MockIo::new(vectored);
        io.max_accept = max_accept;
        let mut codec = codec(io);
        codec
            .send(OutFrame {
                ping: true,
                ..stream_frame(7, syn(), 100, b"payload")
            })
            .await
            .unwrap();
        let writes = writes.lock().unwrap().concat();
        let frames = decode_all(&writes).await;
        assert_eq!(frames.len(), 1);
        let DecodedFrame::Frame(frame) = &frames[0] else {
            panic!()
        };
        assert_eq!(frame.data.as_ref(), b"payload");
    }

    fn t_ping_of(bytes: &[u8]) -> u32 {
        // flags, stream_id, credit, len, then t_ping.
        assert_eq!(bytes[..2], [0x00, 0x39]);
        u32::from_be_bytes(bytes[10..14].try_into().unwrap())
    }

    #[tokio::test(start_paused = true)]
    async fn ping_is_stamped_at_the_first_accepted_byte() {
        let (io, writes, refuse) = MockIo::new(true);
        let mut codec = codec(io);
        // The clock of the codec started at 0.
        tokio::time::advance(Duration::from_micros(1000)).await;

        *refuse.lock().unwrap() = true;
        let frame = OutFrame {
            ping: true,
            ..stream_frame(7, syn(), 100, b"payload")
        };
        let mut send = Box::pin(codec.send(frame));
        assert!(send.as_mut().now_or_never().is_none());
        assert!(writes.lock().unwrap().is_empty());

        // The transport becomes writable later; the stamp is the later time.
        tokio::time::advance(Duration::from_micros(2500)).await;
        send.await.unwrap();
        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(t_ping_of(&writes[0]), 3500);
    }

    #[tokio::test(start_paused = true)]
    async fn header_is_fixed_after_the_first_accepted_byte() {
        let (mut io, writes, _) = MockIo::new(true);
        io.max_accept = 4;
        let mut codec = codec(io);
        tokio::time::advance(Duration::from_micros(10)).await;

        let frame = OutFrame {
            ping: true,
            ..stream_frame(7, syn(), 100, b"payload")
        };
        let mut send = Box::pin(codec.send(frame));
        // Accepts 4 bytes, then the rest 4 at a time, while time passes.
        for _ in 0..3 {
            let _ = send.as_mut().now_or_never();
            tokio::time::advance(Duration::from_micros(100)).await;
        }
        send.await.unwrap();
        let bytes = writes.lock().unwrap().concat();
        assert_eq!(t_ping_of(&bytes), 10);
    }

    fn window(id: u16) -> OutFrame {
        stream_frame(id, ControlFlags::default(), 1000 + u32::from(id), b"")
    }

    fn ping() -> OutFrame {
        OutFrame {
            ping: true,
            ..Default::default()
        }
    }

    /// Returns the `t_ping`s of the given frames.
    fn pings_of(frames: &[DecodedFrame]) -> Vec<u32> {
        frames
            .iter()
            .filter_map(|frame| match frame {
                DecodedFrame::Frame(frame) => frame.header.ping,
                DecodedFrame::Terminate => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn control_frames_share_one_write() {
        let (io, writes, _) = MockIo::new(true);
        let mut codec = codec(io);
        for id in 1..=5 {
            codec.feed(window(id)).await.unwrap();
        }
        codec.flush().await.unwrap();

        let writes = writes.lock().unwrap().clone();
        assert_eq!(writes.len(), 1);
        let sent = (1..=5).map(window).collect::<Vec<_>>();
        assert_all_same(&sent, &decode_all(&writes[0]).await);
    }

    #[tokio::test]
    async fn control_frames_ride_with_the_next_payload() {
        let sent = || {
            vec![
                window(1),
                window(2),
                stream_frame(3, ControlFlags::default(), 0, b"payload"),
            ]
        };
        let (io, writes, _) = MockIo::new(true);
        let mut codec = codec(io);
        for frame in sent() {
            codec.feed(frame).await.unwrap();
        }
        codec.flush().await.unwrap();

        let writes = writes.lock().unwrap().clone();
        assert_eq!(writes.len(), 1);
        assert_all_same(&sent(), &decode_all(&writes[0]).await);
    }

    #[tokio::test]
    async fn a_full_control_buffer_is_written_without_a_flush() {
        let (io, writes, _) = MockIo::new(true);
        let mut codec = codec(io);
        for id in 1..=300 {
            codec.feed(window(id)).await.unwrap();
        }
        assert!(
            !writes.lock().unwrap().is_empty(),
            "the buffer was written before the flush"
        );
        codec.flush().await.unwrap();

        let writes = writes.lock().unwrap().clone();
        for write in &writes {
            assert!(write.len() <= CONTROL_BATCH, "a write of {}", write.len());
        }
        let sent = (1..=300).map(window).collect::<Vec<_>>();
        assert_all_same(&sent, &decode_all(&writes.concat()).await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_buffered_ping_carries_the_time_of_its_write() {
        let (io, writes, refuse) = MockIo::new(true);
        let mut codec = codec(io);
        let clock = codec.clock();
        tokio::time::advance(Duration::from_millis(1)).await;
        let t0 = Instant::now();

        codec.feed(ping()).await.unwrap();
        *refuse.lock().unwrap() = true;
        assert!(codec.flush().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(50)).await;
        codec.flush().await.unwrap();

        let bytes = writes.lock().unwrap().concat();
        let frames = decode_all(&bytes).await;
        assert_eq!(
            pings_of(&frames),
            [clock.micros(t0 + Duration::from_millis(50))]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn two_buffered_pings_are_both_stamped() {
        let (io, writes, refuse) = MockIo::new(true);
        let mut codec = codec(io);
        let clock = codec.clock();
        tokio::time::advance(Duration::from_millis(1)).await;
        let t0 = Instant::now();

        let sent = || vec![ping(), window(1), ping()];
        for frame in sent() {
            codec.feed(frame).await.unwrap();
        }
        *refuse.lock().unwrap() = true;
        assert!(codec.flush().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(50)).await;
        codec.flush().await.unwrap();

        let bytes = writes.lock().unwrap().concat();
        let frames = decode_all(&bytes).await;
        assert_all_same(&sent(), &frames);
        let t_write = clock.micros(t0 + Duration::from_millis(50));
        assert_eq!(pings_of(&frames), [t_write, t_write]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_ping_carries_the_time_of_the_write_that_takes_its_first_byte() {
        let (mut io, writes, _) = MockIo::new(true);
        io.max_accept = 4;
        io.one_write_per_poll = true;
        let mut codec = codec(io);
        let clock = codec.clock();
        tokio::time::advance(Duration::from_millis(1)).await;

        // 8 bytes of `WINDOW`, 6 of `PING`, then the `DATA` with a `PING`.
        let sent = || {
            vec![
                window(1),
                ping(),
                OutFrame {
                    ping: true,
                    ..stream_frame(2, ControlFlags::default(), 0, b"payload")
                },
            ]
        };
        for frame in sent() {
            codec.feed(frame).await.unwrap();
        }
        // 4 bytes per write, 100 us apart.
        let mut writes_at = Vec::new();
        let mut flush = codec.flush();
        loop {
            writes_at.push(Instant::now());
            if (&mut flush).now_or_never().is_some() {
                break;
            }
            tokio::time::advance(Duration::from_micros(100)).await;
        }

        let bytes = writes.lock().unwrap().concat();
        let frames = decode_all(&bytes).await;
        assert_all_same(&sent(), &frames);
        // The third write takes the first byte of the `PING`, the fourth the rest of it and the first bytes
        // of the `DATA`.
        assert_eq!(
            pings_of(&frames),
            [clock.micros(writes_at[2]), clock.micros(writes_at[3])]
        );
    }

    #[tokio::test]
    async fn after_an_unfinished_flush_the_next_frame_waits() {
        let (io, writes, refuse) = MockIo::new(true);
        let mut codec = codec(io);
        codec.feed(window(1)).await.unwrap();
        *refuse.lock().unwrap() = true;
        assert!(codec.flush().now_or_never().is_none());

        // The control buffer has room, but the transport takes no more.
        let mut cx = Context::from_waker(Waker::noop());
        *refuse.lock().unwrap() = true;
        assert!(codec.poll_ready_unpin(&mut cx).is_pending());
        assert!(writes.lock().unwrap().is_empty());
        // Once everything is written, the next frame is taken.
        assert!(matches!(
            codec.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(writes.lock().unwrap().len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn partial_writes_keep_the_frames_intact(#[values(true, false)] vectored: bool) {
        let sent = || {
            vec![
                window(1),
                ping(),
                OutFrame {
                    ping: true,
                    ..stream_frame(2, syn(), 0, b"hello")
                },
                OutFrame {
                    pong: Some(Pong { t_echo: 5 }),
                    ..Default::default()
                },
                stream_frame(
                    3,
                    ControlFlags {
                        fin_write: true,
                        ..Default::default()
                    },
                    0,
                    b"",
                ),
                OutFrame {
                    ping: true,
                    ..window(4)
                },
                stream_frame(5, ControlFlags::default(), 7, b"eleven byte"),
                window(6),
                OutFrame::terminate(),
            ]
        };
        let (mut io, writes, _) = MockIo::new(vectored);
        io.max_accept = 1;
        let mut codec = codec(io);
        for frame in sent() {
            codec.feed(frame).await.unwrap();
        }
        codec.flush().await.unwrap();

        let writes = writes.lock().unwrap().clone();
        assert!(writes.iter().all(|write| write.len() == 1));
        assert_all_same(&sent(), &decode_all(&writes.concat()).await);
    }
}
