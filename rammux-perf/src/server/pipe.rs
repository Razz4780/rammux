use std::{
    io,
    ops::{Not, Range},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, Sink, SinkExt, Stream, StreamExt};

/// Size of the buffer used in the pipes.
pub const BUFFER_SIZE: usize = 64 * 1024;

/// Pipes all data received from an IO duplex back to the sender,
/// then closes the duplex.
pub struct PipeFuturesIo<IO> {
    io: IO,
    buffer: Box<[u8]>,
    /// Fragment of [`Self::buffer`] that was read from [`Self::io`]
    /// and is still waiting to be written back.
    unwritten: Range<usize>,
    /// Whether [`Self::io`] has reached EOF.
    eof: bool,
}

impl<IO> PipeFuturesIo<IO> {
    pub fn new(io: IO) -> Self {
        Self {
            io,
            buffer: vec![0; BUFFER_SIZE].into_boxed_slice(),
            unwritten: 0..0,
            eof: false,
        }
    }
}

impl<IO> Future for PipeFuturesIo<IO>
where
    IO: AsyncWrite + AsyncRead + Unpin,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Top up the buffer with whatever the sender has for us right now.
            // A pending read is not a reason to return, we might still have
            // buffered data to write back.
            if this.eof.not() && this.unwritten.end < BUFFER_SIZE {
                let buf = &mut this.buffer[this.unwritten.end..];
                if let Poll::Ready(result) = Pin::new(&mut this.io).poll_read(cx, buf) {
                    match result? {
                        0 => this.eof = true,
                        read => this.unwritten.end += read,
                    }
                }
            }

            if this.unwritten.is_empty() {
                if this.eof {
                    // This is expected to flush the written data.
                    return Pin::new(&mut this.io).poll_close(cx);
                }
                // The sender may be waiting for its echo before it sends more,
                // so the echo has to reach it before we park.
                std::task::ready!(Pin::new(&mut this.io).poll_flush(cx))?;
                return Poll::Pending;
            }

            let written = std::task::ready!(
                Pin::new(&mut this.io).poll_write(cx, &this.buffer[this.unwritten.clone()])
            )?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            this.unwritten.start += written;
            if this.unwritten.is_empty() {
                // Rewind, so that the next read can use the whole buffer.
                this.unwritten = 0..0;
            }
        }
    }
}

/// Pipes all data received from a [`Bytes`] [`Sink`]+[`Stream`] duplex back to the sender,
/// then closes the duplex.
pub struct PipeBytes<IO> {
    io: IO,
    /// Received data waiting to be written back.
    buffer: BytesMut,
    /// Whether [`Self::io`] has reached EOF.
    eof: bool,
}

impl<IO> PipeBytes<IO> {
    pub fn new(io: IO) -> Self {
        Self {
            io,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            eof: false,
        }
    }
}

impl<IO> Future for PipeBytes<IO>
where
    IO: Stream<Item = io::Result<Bytes>> + Sink<Bytes, Error = io::Error> + Unpin,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Top up the buffer with whatever the sender has for us right now.
            // A pending read is not a reason to return, we might still have
            // buffered data to write back.
            let mut read_pending = false;
            if this.eof.not() && this.buffer.len() < BUFFER_SIZE {
                match this.io.poll_next_unpin(cx) {
                    Poll::Ready(result) => match result.transpose()? {
                        None => this.eof = true,
                        Some(data) => {
                            this.buffer.extend_from_slice(&data);
                        },
                    },
                    Poll::Pending => read_pending = true,
                }
            }

            if this.buffer.is_empty() {
                if this.eof {
                    // This is expected to flush the written data.
                    return Pin::new(&mut this.io).poll_close(cx);
                }
                if read_pending.not() {
                    // We consumed an empty frame, which registered no waker.
                    // Poll again instead of parking for good.
                    continue;
                }
                // The sender may be waiting for its echo before it sends more,
                // so the echo has to reach it before we park.
                std::task::ready!(this.io.poll_flush_unpin(cx))?;
                return Poll::Pending;
            }

            std::task::ready!(this.io.poll_ready_unpin(cx))?;
            this.io.start_send_unpin(this.buffer.split().freeze())?;
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::VecDeque,
        io,
        pin::Pin,
        task::{Context, Poll, Waker, ready},
    };

    use bytes::Bytes;
    use futures::{AsyncRead, AsyncWrite, Sink, Stream};

    use super::{BUFFER_SIZE, PipeBytes, PipeFuturesIo};

    /// Deterministic pseudo-random payload of the given length.
    fn payload(len: usize) -> Vec<u8> {
        let mut state = 0x1234_5678_u32;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                (state >> 24) as u8
            })
            .collect()
    }

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Peer that buffers everything written to it, like [`futures::io::BufWriter`].
    ///
    /// Written data becomes visible to the peer only when the pipe flushes,
    /// which is what makes the missing flushes observable.
    struct MockIo {
        /// Scripted results for [`AsyncRead::poll_read`], one per call.
        ///
        /// An empty [`Vec`] is EOF. A chunk that does not fit in the caller's
        /// buffer is only partially consumed, and the rest is kept for the next call.
        inbound: VecDeque<Poll<Vec<u8>>>,
        /// Maximum number of bytes accepted by a single [`AsyncWrite::poll_write`].
        write_limit: usize,
        /// Written by the pipe, not flushed yet, so not visible to the peer.
        unflushed: Vec<u8>,
        /// Actually delivered to the peer.
        flushed: Vec<u8>,
        closed: bool,
    }

    impl MockIo {
        fn new(inbound: impl IntoIterator<Item = Poll<Vec<u8>>>) -> Self {
            Self {
                inbound: inbound.into_iter().collect(),
                write_limit: usize::MAX,
                unflushed: Default::default(),
                flushed: Default::default(),
                closed: false,
            }
        }

        fn with_write_limit(mut self, write_limit: usize) -> Self {
            self.write_limit = write_limit;
            self
        }
    }

    impl AsyncRead for MockIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let Some(Poll::Ready(mut data)) = this.inbound.pop_front() else {
                return Poll::Pending;
            };
            let read = data.len().min(buf.len());
            buf[..read].copy_from_slice(&data[..read]);
            if read < data.len() {
                this.inbound.push_front(Poll::Ready(data.split_off(read)));
            }
            Poll::Ready(Ok(read))
        }
    }

    impl AsyncWrite for MockIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let written = buf.len().min(this.write_limit);
            this.unflushed.extend_from_slice(&buf[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.flushed.append(&mut this.unflushed);
            Poll::Ready(Ok(()))
        }

        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            ready!(self.as_mut().poll_flush(cx))?;
            self.get_mut().closed = true;
            Poll::Ready(Ok(()))
        }
    }

    /// [`MockIo`] equivalent for the [`Sink`]+[`Stream`] flavor.
    struct MockDuplex {
        /// Scripted results for [`Stream::poll_next`], one per call.
        inbound: VecDeque<Poll<Option<Bytes>>>,
        /// Sent by the pipe, not flushed yet, so not visible to the peer.
        unflushed: Vec<Bytes>,
        /// Actually delivered to the peer.
        flushed: Vec<Bytes>,
        closed: bool,
        /// How many times [`Stream::poll_next`] returned [`Poll::Pending`],
        /// i.e. how many times the pipe registered its waker with us.
        wakers_registered: usize,
    }

    impl MockDuplex {
        fn new(inbound: impl IntoIterator<Item = Poll<Option<Bytes>>>) -> Self {
            Self {
                inbound: inbound.into_iter().collect(),
                unflushed: Default::default(),
                flushed: Default::default(),
                closed: false,
                wakers_registered: 0,
            }
        }

        /// Data the peer has actually seen.
        fn echoed(&self) -> Vec<u8> {
            self.flushed.concat()
        }
    }

    impl Stream for MockDuplex {
        type Item = io::Result<Bytes>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            match this.inbound.pop_front() {
                Some(Poll::Ready(data)) => Poll::Ready(data.map(Ok)),
                Some(Poll::Pending) | None => {
                    this.wakers_registered += 1;
                    Poll::Pending
                },
            }
        }
    }

    impl Sink<Bytes> for MockDuplex {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Bytes) -> io::Result<()> {
            self.get_mut().unflushed.push(item);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.flushed.append(&mut this.unflushed);
            Poll::Ready(Ok(()))
        }

        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            ready!(self.as_mut().poll_flush(cx))?;
            self.get_mut().closed = true;
            Poll::Ready(Ok(()))
        }
    }

    /// Echoes a payload several times larger than [`BUFFER_SIZE`],
    /// with a write limit that forces partial writes.
    ///
    /// Exercises refilling the buffer while a previous read is still being
    /// written back, and rewinding it once it drains.
    #[test]
    fn futures_io_echoes_all_data() {
        let data = payload(3 * BUFFER_SIZE + 1234);
        let mut inbound = data
            .chunks(3 * 1024)
            .map(|chunk| Poll::Ready(chunk.to_vec()))
            .collect::<VecDeque<_>>();
        inbound.push_back(Poll::Ready(Vec::new()));
        let mut pipe = PipeFuturesIo::new(MockIo::new(inbound).with_write_limit(997));

        assert!(matches!(poll_once(&mut pipe), Poll::Ready(Ok(()))));
        assert_eq!(pipe.io.flushed, data);
        assert!(pipe.io.closed);
    }

    /// The sender goes quiet after its first chunk, waiting for the echo.
    ///
    /// The echo has to be flushed before the pipe parks, or both sides wait forever.
    #[test]
    fn futures_io_flushes_echo_before_parking() {
        let mut pipe =
            PipeFuturesIo::new(MockIo::new([Poll::Ready(b"hello".to_vec()), Poll::Pending]));

        assert!(poll_once(&mut pipe).is_pending());
        assert_eq!(
            pipe.io.flushed, b"hello",
            "echo did not reach the peer, still sitting in the writer: {:?}",
            pipe.io.unflushed,
        );
    }

    /// A writer that accepts nothing must not spin the pipe forever.
    #[test]
    fn futures_io_rejects_a_stuck_writer() {
        let mut pipe =
            PipeFuturesIo::new(MockIo::new([Poll::Ready(b"hello".to_vec())]).with_write_limit(0));

        let Poll::Ready(Err(error)) = poll_once(&mut pipe) else {
            panic!("expected a write failure");
        };
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
    }

    /// [`futures_io_echoes_all_data`] for the [`Sink`]+[`Stream`] flavor.
    #[test]
    fn bytes_echoes_all_data() {
        let data = payload(3 * BUFFER_SIZE + 1234);
        let mut inbound = data
            .chunks(3 * 1024)
            .map(|chunk| Poll::Ready(Some(Bytes::copy_from_slice(chunk))))
            .collect::<VecDeque<_>>();
        inbound.push_back(Poll::Ready(None));
        let mut pipe = PipeBytes::new(MockDuplex::new(inbound));

        assert!(matches!(poll_once(&mut pipe), Poll::Ready(Ok(()))));
        assert_eq!(pipe.io.echoed(), data);
        assert!(pipe.io.closed);
    }

    /// [`futures_io_flushes_echo_before_parking`] for the [`Sink`]+[`Stream`] flavor.
    #[test]
    fn bytes_flushes_echo_before_parking() {
        let mut pipe = PipeBytes::new(MockDuplex::new([
            Poll::Ready(Some(Bytes::from_static(b"hello"))),
            Poll::Pending,
        ]));

        assert!(poll_once(&mut pipe).is_pending());
        assert_eq!(
            pipe.io.echoed(),
            b"hello",
            "echo did not reach the peer, still sitting in the sink: {:?}",
            pipe.io.unflushed,
        );
    }

    /// An empty frame is progress, but it registers no waker.
    ///
    /// The pipe has to poll again instead of parking, or it never wakes up.
    #[test]
    fn bytes_survives_an_empty_frame() {
        let mut pipe = PipeBytes::new(MockDuplex::new([
            Poll::Ready(Some(Bytes::new())),
            Poll::Ready(Some(Bytes::from_static(b"data"))),
            Poll::Pending,
        ]));

        assert!(poll_once(&mut pipe).is_pending());
        assert!(
            pipe.io.wakers_registered > 0,
            "parked without registering a waker, so nothing can ever wake the pipe",
        );
        assert_eq!(
            pipe.io.echoed(),
            b"data",
            "dropped the frame after the empty one"
        );
    }
}
