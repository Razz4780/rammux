use std::{
    collections::VecDeque,
    io,
    num::NonZeroU32,
    ops::Not,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Buf;
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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

/// How many [`EncoderItem`]s can be stored in a [`RammuxCodec`] before data has to be written into the IO transport.
pub const ENCODER_QUEUE_CAPACITY: usize = 16;

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
/// # Encoder queue
///
/// This codes queues [`EncoderItem`]s to leverage vectored writes for better performance.
/// At most [`ENCODER_QUEUE_CAPACITY`] items can be queued before data has to be written into the IO transport.
pub struct RammuxCodec<IO> {
    io: IO,
    encoder_queue: VecDeque<EncoderItem>,
    decoder: DecoderState,
    frame_limit: NonZeroU32,
}

impl<IO> RammuxCodec<IO> {
    pub fn new(io: IO, frame_limit: NonZeroU32) -> Self {
        Self {
            io,
            encoder_queue: VecDeque::with_capacity(ENCODER_QUEUE_CAPACITY),
            decoder: Default::default(),
            frame_limit,
        }
    }

    pub fn into_inner(self) -> IO {
        self.io
    }
}

impl<IO> Stream for RammuxCodec<IO>
where
    IO: AsyncRead + Unpin,
{
    type Item = Result<DecodedFrame, ErrorKind>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.decoder {
                DecoderState::ReadingHeader { filled, buffer } => {
                    let mut buf = ReadBuf::new(&mut buffer[*filled..]);
                    std::task::ready!(Pin::new(&mut this.io).poll_read(cx, &mut buf))?;
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
                            this.decoder = Default::default();
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
                            this.decoder = Default::default();
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
                            Some(len) if len > this.frame_limit => {
                                break Poll::Ready(Some(Err(ErrorKind::Stream {
                                    id: stream_id,
                                    error: "sent a DATA frame exceeding the configured size limit"
                                        .into(),
                                })));
                            },
                            Some(len) => {
                                this.decoder = DecoderState::ReadingData {
                                    stream_id,
                                    flags,
                                    filled: 0,
                                    buffer: Buffer::with_capactiy(crate::safe_cast_usize(
                                        len.get(),
                                    )),
                                };
                            },
                            None => {
                                this.decoder = Default::default();
                                break Poll::Ready(Some(Ok(DecodedFrame::Stream {
                                    stream_id,
                                    flags,
                                    payload: StreamPayload::Data(Default::default()),
                                })));
                            },
                        },
                        Header::Term => {
                            this.decoder = Default::default();
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

                    std::task::ready!(Pin::new(&mut this.io).poll_read(cx, &mut read_buf))?;
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
                        this.decoder = Default::default();
                        break Poll::Ready(Some(Ok(output)));
                    }
                },
            }
        }
    }
}

impl<IO> RammuxCodec<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn encoder_queue_full(&self) -> bool {
        self.encoder_queue.len() >= ENCODER_QUEUE_CAPACITY
    }

    fn poll_encode_while<F>(
        &mut self,
        cx: &mut Context<'_>,
        mut pred: F,
    ) -> Poll<Result<(), ErrorKind>>
    where
        F: FnMut(&Self) -> bool,
    {
        while pred(self) {
            let mut io_vecs = [io::IoSlice::new(&[]); ENCODER_QUEUE_CAPACITY * 2];
            let mut n_vecs = 0;
            for item in &self.encoder_queue {
                n_vecs += item.chunks_vectored(&mut io_vecs[n_vecs..]);
            }
            let mut written = std::task::ready!(
                Pin::new(&mut self.io).poll_write_vectored(cx, &io_vecs[..n_vecs])
            )?;

            while written > 0 {
                self.encoder_queue.pop_front_if(|item| {
                    let advance = written.min(item.remaining());
                    written -= advance;
                    item.advance(advance);
                    item.has_remaining().not()
                });
            }
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
        self.get_mut()
            .poll_encode_while(cx, Self::encoder_queue_full)
    }

    fn start_send(self: Pin<&mut Self>, item: EncoderItem) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.encoder_queue_full() {
            panic!("frame encoder is busy");
        }
        this.encoder_queue.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_encode_while(cx, |this| this.encoder_queue.is_empty().not()))?;
        Pin::new(&mut this.io)
            .poll_flush(cx)
            .map_err(ErrorKind::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_encode_while(cx, |this| this.encoder_queue.is_empty().not()))?;
        Pin::new(&mut this.io)
            .poll_shutdown(cx)
            .map_err(ErrorKind::from)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

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
        let mut codec_1 = RammuxCodec::new(io_1, frame_limit);
        let mut codec_2 = RammuxCodec::new(io_2, frame_limit);

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
        let mut codec_1 = RammuxCodec::new(io_1, frame_limit);
        let mut codec_2 = RammuxCodec::new(io_2, frame_limit);

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
