//! Adapters that give every multiplexer's streams the one shape the rest of
//! this crate speaks: a [`Sink`] of [`Bytes`] and a fallible [`Stream`] of
//! [`Bytes`], with [`io::Error`] on both.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
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
