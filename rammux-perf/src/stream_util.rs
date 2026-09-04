//! Adapters that give every multiplexer's streams the one shape the rest of
//! this crate speaks: a [`Sink`] of [`Bytes`] and a fallible [`Stream`] of
//! [`Bytes`], with [`io::Error`] on both.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt, channel::mpsc};
use hyper::body::{Body, Frame};
use rammux::stream::RammuxDuplex;

/// [`RammuxDuplex`] with a fallible item type.
///
/// A rammux stream never fails on the read side, so its [`Stream`] is
/// infallible; this wrapper only adds the `Ok`.
pub struct RammuxIo(pub RammuxDuplex);

impl Stream for RammuxIo {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .0
            .poll_next_unpin(cx)
            .map(|data| data.map(Ok))
    }
}

impl Sink<Bytes> for RammuxIo {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.get_mut().0.start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().0.poll_close_unpin(cx)
    }
}

/// An HTTP body fed from a channel: one direction of an HTTP/2 stream.
///
/// The server sends its response body through one of these, the client its
/// request body. Dropping or closing the sender ends the body.
pub struct ChannelBody(pub mpsc::Receiver<Bytes>);

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.get_mut()
            .0
            .poll_next_unpin(cx)
            .map(|opt| opt.map(Frame::data).map(Ok))
    }
}

/// How much the read half asks for at a time.
const QUIC_READ_CHUNK: usize = 64 * 1024;

/// One QUIC bidirectional stream, as a [`Bytes`] duplex.
///
/// Both sides use it: the client for a stream it opened, the server for one it
/// accepted. QUIC's two halves are separate objects, so this is where they are
/// put back together into the shape the rest of the crate uses.
pub struct QuicDuplex {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    /// What [`Sink::start_send`] took, minus whatever has reached `quinn`.
    unsent: Bytes,
    /// Where the read half accumulates before handing a chunk out.
    buffer: BytesMut,
    /// Whether the peer has finished sending.
    finished: bool,
}

impl QuicDuplex {
    pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send,
            recv,
            unsent: Bytes::new(),
            buffer: BytesMut::new(),
            finished: false,
        }
    }

    /// Hands as much of [`Self::unsent`] to `quinn` as it will take.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.unsent.is_empty() {
            let written = std::task::ready!(Pin::new(&mut self.send).poll_write(cx, &self.unsent))
                .map_err(io::Error::other)?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.unsent.advance(written);
        }
        Poll::Ready(Ok(()))
    }
}

impl Stream for QuicDuplex {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        this.buffer.reserve(QUIC_READ_CHUNK);
        let read = std::task::ready!(tokio_util::io::poll_read_buf(
            Pin::new(&mut this.recv),
            cx,
            &mut this.buffer,
        ))?;
        if read == 0 {
            this.finished = true;
            return Poll::Ready(None);
        }
        Poll::Ready(Some(Ok(this.buffer.split().freeze())))
    }
}

impl Sink<Bytes> for QuicDuplex {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_drain(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.unsent.is_empty() {
            this.unsent = item;
            Ok(())
        } else {
            Err(io::Error::other("not ready"))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Once `quinn` has the bytes they are on their way; there is no
        // buffer of our own left to push.
        self.get_mut().poll_drain(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_drain(cx))?;
        // Sends the FIN, which the echo peer reads as EOF.
        match this.send.finish() {
            // Already finished: closing twice is not an error here.
            Ok(()) | Err(quinn::ClosedStream { .. }) => Poll::Ready(Ok(())),
        }
    }
}
