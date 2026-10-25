use std::{
    io,
    num::NonZeroU32,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Buf;
use futures::{FutureExt, Sink, Stream};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};

use crate::{
    buffer::Buffer,
    codec::{
        decoder::{DecodedFrame, DecoderState, StreamPayload},
        encoder::EncoderItem,
    },
    error::ErrorKind,
    header::{Header, RawHeader},
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
/// Yielded [`DecodedFrame`]s can have arbitrary values, except that [`StreamPayload::Data`]
/// is never larger than the configured frame limit.
///
/// # Encoder
///
/// This codec holds at most one [`EncoderItem`]. It is written to the IO transport completely
/// (as a single vectored write of the header and the payload, when the transport allows)
/// before the next item can be sent. There is no queue, so the caller decides what goes next.
pub struct RammuxCodec<IO> {
    io: IO,
    pending: Option<EncoderItem>,
    decoder: DecoderState,
    frame_limit: NonZeroU32,
    inbound_frame_sleep: Pin<Box<Sleep>>,
    inbound_frame_timeout: Duration,
}

impl<IO> RammuxCodec<IO> {
    pub fn new(io: IO, frame_limit: NonZeroU32, inound_frame_timeout: Duration) -> Self {
        Self {
            io,
            pending: None,
            decoder: Default::default(),
            frame_limit,
            inbound_frame_sleep: Box::pin(tokio::time::sleep(inound_frame_timeout)),
            inbound_frame_timeout: inound_frame_timeout,
        }
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
                DecoderState::ReadingHeader { filled, buffer } => {
                    let mut buf = ReadBuf::new(&mut buffer[*filled..]);
                    std::task::ready!(Pin::new(&mut self.io).poll_read(cx, &mut buf))?;
                    match buf.filled().len() {
                        0 if *filled == 0 => break Poll::Ready(None),
                        0 => break Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into()))),
                        n => {
                            *filled += n;
                            if *filled < buffer.len() {
                                continue;
                            }
                        },
                    }

                    let raw_header = RawHeader::decode(*buffer);
                    let header = raw_header.validate()?;
                    match header {
                        Header::Ping {
                            payload,
                            is_response,
                        } => {
                            self.decoder = Default::default();
                            break Poll::Ready(Some(Ok(DecodedFrame::Ping {
                                payload,
                                is_response,
                            })));
                        },
                        Header::WindowUpdate {
                            stream_id,
                            flags,
                            len,
                        } => {
                            self.decoder = Default::default();
                            break Poll::Ready(Some(Ok(DecodedFrame::Stream {
                                stream_id,
                                flags,
                                payload: StreamPayload::WindowUpdate(len),
                            })));
                        },
                        Header::Data {
                            stream_id,
                            flags,
                            len,
                        } => match NonZeroU32::new(len) {
                            Some(len) if len > self.frame_limit => {
                                break Poll::Ready(Some(Err(ErrorKind::Stream {
                                    id: stream_id,
                                    error: "sent a DATA frame exceeding the configured size limit"
                                        .into(),
                                })));
                            },
                            Some(len) => {
                                self.decoder = DecoderState::ReadingData {
                                    stream_id,
                                    flags,
                                    filled: 0,
                                    buffer: Buffer::with_capactiy(crate::safe_cast_usize(
                                        len.get(),
                                    )),
                                };
                            },
                            None => {
                                self.decoder = Default::default();
                                break Poll::Ready(Some(Ok(DecodedFrame::Stream {
                                    stream_id,
                                    flags,
                                    payload: StreamPayload::Data(Default::default()),
                                })));
                            },
                        },
                        Header::Term => {
                            self.decoder = Default::default();
                            break Poll::Ready(Some(Ok(DecodedFrame::Terminate)));
                        },
                    }
                },

                DecoderState::ReadingData {
                    stream_id,
                    flags,
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
                        let output = DecodedFrame::Stream {
                            stream_id: *stream_id,
                            flags: *flags,
                            payload: StreamPayload::Data(data),
                        };
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
            if !item.has_remaining() {
                self.pending = None;
                break;
            }
            let mut io_vecs = [io::IoSlice::new(&[]); 2];
            let n_vecs = item.chunks_vectored(&mut io_vecs);
            let written = std::task::ready!(
                Pin::new(&mut self.io).poll_write_vectored(cx, &io_vecs[..n_vecs])
            )?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            item.advance(written);
        }

        Poll::Ready(Ok(()))
    }
}

impl<IO> Sink<EncoderItem> for RammuxCodec<IO>
where
    IO: AsyncWrite + Unpin,
{
    type Error = ErrorKind;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_write_pending(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: EncoderItem) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.pending.is_some() {
            panic!("frame encoder is busy");
        }
        this.pending = Some(item);
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
    use std::{num::NonZeroU32, time::Duration};

    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};

    use crate::{
        StreamId,
        buffer::Buffer,
        codec::{DecodedFrame, EncoderItem, RammuxCodec, StreamPayload},
        header::{ControlFlags, PingPayload},
    };

    #[tokio::test]
    async fn encode_decode() {
        let data = {
            let mut buffer = Buffer::with_capactiy(5);
            unsafe {
                buffer
                    .as_mut_slice()
                    .assume_init_mut()
                    .copy_from_slice(b"hello");
                buffer.assume_init()
            }
        };
        let frames = [
            DecodedFrame::Ping {
                payload: PingPayload::random(),
                is_response: false,
            },
            DecodedFrame::Ping {
                payload: PingPayload::random(),
                is_response: true,
            },
            DecodedFrame::Stream {
                stream_id: StreamId::from_be_bytes([1, 1, 1]),
                flags: ControlFlags {
                    syn: true,
                    fin_read: false,
                    fin_write: false,
                },
                payload: StreamPayload::WindowUpdate(2137),
            },
            DecodedFrame::Stream {
                stream_id: StreamId::from_be_bytes([2, 2, 2]),
                flags: ControlFlags {
                    syn: false,
                    fin_read: true,
                    fin_write: false,
                },
                payload: StreamPayload::Data(data),
            },
            DecodedFrame::Stream {
                stream_id: StreamId::from_be_bytes([3, 3, 3]),
                flags: ControlFlags {
                    syn: false,
                    fin_read: false,
                    fin_write: true,
                },
                payload: StreamPayload::WindowUpdate(0),
            },
            DecodedFrame::Stream {
                stream_id: StreamId::from_be_bytes([4, 4, 4]),
                flags: ControlFlags {
                    syn: true,
                    fin_read: true,
                    fin_write: true,
                },
                payload: StreamPayload::Data(Default::default()),
            },
            DecodedFrame::Terminate,
        ];

        let (io_1, io_2) = tokio::io::duplex(8);
        let frame_limit = NonZeroU32::new(11).unwrap();
        let mut codec_1 = RammuxCodec::new(io_1, frame_limit, Duration::from_secs(1));
        let mut codec_2 = RammuxCodec::new(io_2, frame_limit, Duration::from_secs(1));

        tokio::join!(
            async {
                for frame in &frames {
                    let item = match frame {
                        DecodedFrame::Ping {
                            payload,
                            is_response,
                        } => EncoderItem::new_ping(*payload, *is_response),
                        DecodedFrame::Stream {
                            stream_id,
                            flags,
                            payload,
                        } => match payload {
                            StreamPayload::WindowUpdate(update) => {
                                EncoderItem::new_window_update(*stream_id, *flags, *update)
                            },
                            StreamPayload::Data(data) => {
                                EncoderItem::new_data(*stream_id, *flags, data.clone().into())
                            },
                        },
                        DecodedFrame::Terminate => EncoderItem::new_terminate(),
                    };
                    codec_1.feed(item).await.unwrap();
                }
                codec_1.close().await.unwrap();
            },
            async {
                for frame in &frames {
                    let received = codec_2.next().await.unwrap().unwrap();
                    assert_eq!(received, *frame,);
                }
                assert!(codec_2.next().await.is_none());
            },
        );
    }

    #[tokio::test]
    async fn frame_limit() {
        let (io_1, io_2) = tokio::io::duplex(32);
        let frame_limit = NonZeroU32::new(8).unwrap();
        let mut codec_1 = RammuxCodec::new(io_1, frame_limit, Duration::from_secs(1));
        let mut codec_2 = RammuxCodec::new(io_2, frame_limit, Duration::from_secs(1));

        codec_1
            .send(EncoderItem::new_data(
                StreamId::from_be_bytes([1, 2, 3]),
                Default::default(),
                Bytes::from_static(b"123456789"),
            ))
            .await
            .unwrap();
        codec_2.next().await.unwrap().unwrap_err();
    }
}
