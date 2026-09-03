//! Adapters that give every multiplexer's streams the one shape the rest of
//! this crate speaks: a [`Sink`] of [`Bytes`] and a fallible [`Stream`] of
//! [`Bytes`], with [`io::Error`] on both.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use h2::{RecvStream, SendStream, client::ResponseFuture};
use hyper::StatusCode;
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

/// One HTTP/2 stream as a byte duplex.
///
/// The `h2` crate hands the two halves out separately and leaves both flow
/// control windows to the caller: the peer may not send past the receive
/// window until the bytes are released, and nothing goes out until the
/// connection has assigned the stream some send capacity. Both sides of the
/// benchmark want the same duplex on top of that, so it lives here - the
/// server gets a stream it accepted, the client one it opened.
pub struct H2Duplex {
    recv: Recv,
    send: SendStream<Bytes>,
    /// What [`Sink::start_send`] took, minus whatever has been handed to `h2`
    /// so far. Send capacity arrives in whatever increments the peer's window
    /// allows, which is rarely a whole item.
    unsent: Bytes,
    /// Whether the send half has been closed already.
    closed: bool,
}

/// The receiving half, which the client only gets once its response arrives.
enum Recv {
    /// Client side, before the response headers.
    Response(ResponseFuture),
    /// The body, once there is one.
    Body(RecvStream),
    /// EOF, or a response that will not carry a body.
    Done,
}

impl H2Duplex {
    /// The server's side of a stream: the request body is open already.
    pub fn accepted(recv: RecvStream, send: SendStream<Bytes>) -> Self {
        Self::new(Recv::Body(recv), send)
    }

    /// The client's side of a stream: the body comes with the response, so
    /// the first read waits for it.
    pub fn requested(response: ResponseFuture, send: SendStream<Bytes>) -> Self {
        Self::new(Recv::Response(response), send)
    }

    fn new(recv: Recv, send: SendStream<Bytes>) -> Self {
        Self {
            recv,
            send,
            unsent: Bytes::new(),
            closed: false,
        }
    }

    /// Hands as much of [`Self::unsent`] to `h2` as the peer's window allows.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            if self.unsent.is_empty() {
                break Poll::Ready(Ok(()));
            }
            // Capacity has to be asked for, and only what has been assigned
            // can be sent: `send_data` past it buffers without bound.
            self.send.reserve_capacity(self.unsent.len());
            let capacity = self.send.capacity();
            if capacity == 0 {
                match std::task::ready!(self.send.poll_capacity(cx)) {
                    // Capacity was assigned; how much is what `capacity` says.
                    Some(Ok(..)) => continue,
                    Some(Err(error)) => break Poll::Ready(Err(io::Error::other(error))),
                    // The stream is gone, so no capacity is ever coming.
                    None => break Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                }
            }
            let chunk = self.unsent.split_to(capacity.min(self.unsent.len()));
            self.send
                .send_data(chunk, false)
                .map_err(io::Error::other)?;
        }
    }
}

impl Stream for H2Duplex {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.recv {
                Recv::Response(response) => {
                    let response =
                        std::task::ready!(response.poll_unpin(cx)).map_err(io::Error::other)?;
                    if response.status() != StatusCode::OK {
                        this.recv = Recv::Done;
                        break Poll::Ready(Some(Err(io::Error::other(format!(
                            "server responded with {}",
                            response.status()
                        )))));
                    }
                    this.recv = Recv::Body(response.into_body());
                },
                Recv::Body(body) => {
                    let Some(result) = std::task::ready!(body.poll_data(cx)) else {
                        this.recv = Recv::Done;
                        break Poll::Ready(None);
                    };
                    let data = result.map_err(io::Error::other)?;
                    // The peer may not send past the window until these bytes
                    // are accounted for, and they are: the caller gets them
                    // right here.
                    body.flow_control()
                        .release_capacity(data.len())
                        .map_err(io::Error::other)?;
                    break Poll::Ready(Some(Ok(data)));
                },
                Recv::Done => break Poll::Ready(None),
            }
        }
    }
}

impl Sink<Bytes> for H2Duplex {
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
        // Frames reach the wire when the connection is polled, which is the
        // connection driver's job; there is nothing to push from here.
        self.get_mut().poll_drain(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_drain(cx))?;
        if !this.closed {
            this.closed = true;
            // An empty END_STREAM frame, which the peer reads as EOF.
            this.send
                .send_data(Bytes::new(), true)
                .map_err(io::Error::other)?;
        }
        Poll::Ready(Ok(()))
    }
}
