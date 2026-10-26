use std::{
    io,
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
    clock::{Clock, PingStamps},
    codec::{
        decoder::{DecodedFrame, DecoderState, InboundFrame},
        encoder::{EncoderItem, OutFrame},
    },
    error::{DecodeError, ErrorKind},
    header::{Flags, Header},
};

pub mod decoder;
pub mod encoder;

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
/// This codec holds at most one frame. It is written to the IO transport completely
/// (as one `poll_write_vectored` of the header and the payload) before the next frame can be sent.
/// There is no queue, so the caller decides what goes next.
///
/// The header is encoded when the transport is first asked to write the frame,
/// and encoded again if the transport accepts nothing, so that the `PING` timestamp
/// is the time of the first accepted byte.
///
/// A transport without native vectored writes (`is_write_vectored()` is false) gets tokio's default
/// `poll_write_vectored`, which writes the header, and the payload with the next call: two writes per frame.
/// Such a transport is best wrapped in a buffered writer (e.g. `tokio::io::BufWriter`), which this codec flushes
/// after the last frame it has ready.
///
/// # Decoder
///
/// A header has a variable length that the flags (its first 2 bytes) determine, and the codec never reads past
/// the end of a frame, so a `DATA` frame takes three reads of the transport: the flags, the rest of the header,
/// and the payload (a control frame takes two). On a bare socket each is a system call, so wrap the transport in
/// a buffered reader too (`tokio::io::BufStream` does both directions). What a buffered reader holds stays in
/// the transport returned by the downgrade.
pub struct RammuxCodec<IO> {
    io: IO,
    pending: Option<EncoderItem>,
    decoder: DecoderState,
    frame_limit: NonZeroU16,
    clock: Clock,
    ping_stamps: PingStamps,
    inbound_frame_sleep: Pin<Box<Sleep>>,
    inbound_frame_timeout: Duration,
}

impl<IO> RammuxCodec<IO> {
    pub fn new(
        io: IO,
        frame_limit: NonZeroU16,
        inbound_frame_timeout: Duration,
        clock: Clock,
    ) -> Self {
        Self {
            io,
            pending: None,
            decoder: Default::default(),
            frame_limit,
            clock,
            ping_stamps: Default::default(),
            inbound_frame_sleep: Box::pin(tokio::time::sleep(inbound_frame_timeout)),
            inbound_frame_timeout,
        }
    }

    /// Returns the `t_ping` of the newest `PING` that went out, if any.
    pub fn newest_ping_stamp(&self) -> Option<u32> {
        self.ping_stamps.newest()
    }

    pub fn into_inner(self) -> IO {
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
                        buffer: Buffer::with_capactiy(usize::from(len.get())),
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
                    .reset(Instant::now() + this.inbound_frame_timeout);
                Poll::Ready(result)
            },
        }
    }
}

impl<IO> RammuxCodec<IO>
where
    IO: AsyncWrite + Unpin,
{
    /// Writes the pending item (if any) to the transport completely.
    fn poll_write_pending(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        while let Some(item) = &mut self.pending {
            if item.remaining() == 0 {
                self.pending = None;
                break;
            }

            // Until the transport accepts a byte of the frame, the timestamp is whatever the time is now.
            let stamp = (item.has_ping() && !item.is_started()).then(|| {
                let stamp = self.ping_stamps.next(self.clock.micros(Instant::now()));
                item.stamp(stamp);
                stamp
            });
            // The header and the payload go to the transport together. A transport without
            // vectored writes gets them one after another, which is the best it can do.
            let (slices, n_slices) = item.slices();
            let written = std::task::ready!(
                Pin::new(&mut self.io).poll_write_vectored(cx, &slices[..n_slices])
            )?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            if let Some(stamp) = stamp {
                self.ping_stamps.commit(stamp);
            }
            item.advance(written);
        }

        Poll::Ready(Ok(()))
    }
}

impl<IO> Sink<OutFrame> for RammuxCodec<IO>
where
    IO: AsyncWrite + Unpin,
{
    type Error = ErrorKind;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_write_pending(cx)
    }

    fn start_send(self: Pin<&mut Self>, frame: OutFrame) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.pending.is_some() {
            panic!("frame encoder is busy");
        }
        assert!(!frame.is_empty(), "empty frame");
        this.pending = Some(EncoderItem::new(frame));
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_write_pending(cx))?;
        Pin::new(&mut this.io)
            .poll_flush(cx)
            .map_err(ErrorKind::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_write_pending(cx))?;
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
        task::{Context, Poll},
        time::Duration,
    };

    use bytes::Bytes;
    use futures::{FutureExt, SinkExt, StreamExt};
    use rstest::rstest;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

    use crate::{
        StreamId,
        clock::Clock,
        codec::{
            RammuxCodec,
            decoder::DecodedFrame,
            encoder::{OutFrame, OutStream},
        },
        header::{ControlFlags, Pong},
    };

    const LIMIT: NonZeroU16 = NonZeroU16::new(11).unwrap();

    fn codec<IO>(io: IO) -> RammuxCodec<IO> {
        RammuxCodec::new(io, LIMIT, Duration::from_secs(1), Clock::new())
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
            };
            (io, writes, refuse)
        }

        fn write(&mut self, cx: &mut Context<'_>, bufs: &[&[u8]]) -> Poll<io::Result<usize>> {
            if std::mem::take(&mut *self.refuse.lock().unwrap()) {
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
            _: &mut tokio::io::ReadBuf<'_>,
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
                    pong: Some(Pong {
                        t_echo: 1,
                        t_read: u32::MAX,
                        rx_bytes: 3,
                    }),
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
                    pong: Some(Pong {
                        t_echo: 9,
                        t_read: 8,
                        rx_bytes: 7,
                    }),
                    ..stream_frame(
                        0x0404,
                        ControlFlags {
                            syn: true,
                            fin_read: true,
                            fin_write: true,
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
                    let DecodedFrame::Frame(received) = codec_2.next().await.unwrap().unwrap()
                    else {
                        assert!(sent.term);
                        continue;
                    };
                    assert_eq!(received.header.ping.is_some(), sent.ping);
                    assert_eq!(received.header.pong, sent.pong);
                    match sent.stream {
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
                assert!(codec_2.next().await.is_none());
            },
        );
    }

    /// Feeds raw bytes to a codec and returns the first result.
    async fn decode(bytes: &[u8]) -> Option<Result<DecodedFrame, crate::error::ErrorKind>> {
        let (mut writer, reader) = tokio::io::duplex(256);
        writer.write_all(bytes).await.unwrap();
        writer.shutdown().await.unwrap();
        codec(reader).next().await
    }

    #[rstest]
    #[case::reserved_bits(&[0x02, 0x10, 0, 1, 0, 1, b'a'])]
    #[case::reserved_bit_0x0080(&[0x00, 0x90, 0, 1, 0, 1, b'a'])]
    #[case::high_reserved_bits(&[0x80, 0x10, 0, 1, 0, 1, b'a'])]
    #[case::no_flags(&[0x00, 0x00])]
    #[case::term_and_ping(&[0x01, 0x20, 0, 0, 0, 1])]
    #[case::term_and_stream(&[0x01, 0x04, 0, 1])]
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
        assert!(
            matches!(error, crate::error::ErrorKind::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof)
        );
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
            0x01, 0x02, 0x03, 0x04, // t_read
            0xF0, 0xF1, 0xF2, 0xF3, // rx_bytes
            b'o', b'k',
        ];
        let Some(Ok(DecodedFrame::Frame(frame))) = decode(&bytes).await else {
            panic!("expected a frame");
        };
        assert_eq!(frame.header.ping, Some(0x99AABBCC));
        assert_eq!(
            frame.header.pong,
            Some(Pong {
                t_echo: 0x0A0B0C0D,
                t_read: 0x01020304,
                rx_bytes: 0xF0F1F2F3
            })
        );
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
    async fn ping_stamps_strictly_increase() {
        let (io, writes, _) = MockIo::new(true);
        let mut codec = codec(io);
        tokio::time::advance(Duration::from_micros(40)).await;
        for _ in 0..3 {
            // The paused clock does not move between the frames.
            codec
                .send(OutFrame {
                    ping: true,
                    ..stream_frame(7, syn(), 100, b"x")
                })
                .await
                .unwrap();
        }
        let stamps = writes
            .lock()
            .unwrap()
            .iter()
            .map(|w| t_ping_of(w))
            .collect::<Vec<_>>();
        assert_eq!(stamps, [40, 41, 42]);
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
        // The transport accepts 4 bytes per write, and the clock moves between the attempts: a header
        // that was encoded again at every attempt would carry a later `PING` timestamp.
        for _ in 0..3 {
            let _ = send.as_mut().now_or_never();
            tokio::time::advance(Duration::from_micros(100)).await;
        }
        send.await.unwrap();
        let bytes = writes.lock().unwrap().concat();
        assert_eq!(t_ping_of(&bytes), 10);
    }
}
