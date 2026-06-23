use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::{StreamId, rr_bus::Node, stream::StreamHandle};

/// Bidirectional virtual data stream created within a Rammux connection.
///
/// Dropping it will close the stream.
///
/// # Flushing
///
/// [`Sink::poll_flush`] and [`Sink::poll_close`] return [`Poll::Ready`] as soon as all pending data
/// is framed and enqueued for sending through the IO transport.
/// If the connection is downgraded ([`RammuxConnection::downgrade`](crate::connection::RammuxConnection::downgrade))
/// before the data is enqueued, the data will be lost.
/// Note that the data can still be lost if the connection fails.
pub struct RammuxDuplex {
    pub(super) sink: RammuxSink,
    pub(super) stream: RammuxStream,
}

impl RammuxDuplex {
    /// Returns the ID of this stream.
    pub fn id(&self) -> StreamId {
        self.sink.id
    }

    /// Splits this stream into independent [`Sink`] and [`Stream`] handles.
    pub fn into_split(self) -> (RammuxSink, RammuxStream) {
        (self.sink, self.stream)
    }
}

impl Stream for RammuxDuplex {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx)
    }
}

impl Sink<Bytes> for RammuxDuplex {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.get_mut().sink.start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_close_unpin(cx)
    }
}

/// [`Sink`] half of a [`RammuxDuplex`].
///
/// Dropping it will close writing on the stream.
///
/// # Flushing
///
/// [`Sink::poll_flush`] and [`Sink::poll_close`] return [`Poll::Ready`] as soon as all pending data
/// is framed and enqueued for sending through the IO transport.
/// If the connection is downgraded ([`RammuxConnection::downgrade`](crate::connection::RammuxConnection::downgrade))
/// before the data is enqueued, the data will be lost.
/// Note that the data can still be lost if the connection fails.
pub struct RammuxSink {
    pub(super) id: StreamId,
    pub(super) node: Option<Node<StreamHandle>>,
}

impl RammuxSink {
    /// Returns the ID of this stream.
    pub fn id(&self) -> StreamId {
        self.id
    }
}

impl Sink<Bytes> for RammuxSink {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.node
            .as_ref()
            .ok_or(io::ErrorKind::BrokenPipe)?
            .inspect(|handle| handle.outbound.poll_write_ready(cx.waker()))
            .map_err(|error| {
                this.node = None;
                error
            })
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.node
            .as_ref()
            .ok_or(io::ErrorKind::BrokenPipe)?
            .modify(|handle| handle.outbound.write(item))
            .inspect_err(|_| {
                this.node = None;
            })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .node
            .as_ref()
            .map(|node| node.inspect(|handle| handle.outbound.poll_flushed(cx.waker())))
            .unwrap_or(Poll::Ready(()))
            .map(Ok)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let Some(node) = &this.node else {
            return Poll::Ready(Ok(()));
        };
        node.modify(|handle| {
            handle.outbound.close_writing();
            handle.outbound.poll_flushed(cx.waker())
        })
        .map(|()| {
            this.node = None;
            Ok(())
        })
    }
}

impl Drop for RammuxSink {
    fn drop(&mut self) {
        let Some(node) = self.node.take() else {
            return;
        };
        node.modify(|handle| handle.outbound.close_writing());
    }
}

/// [`Stream`] half of a [`RammuxDuplex`].
///
/// Dropping it will close reading on the stream.
pub struct RammuxStream {
    pub(super) id: StreamId,
    pub(super) node: Option<Node<StreamHandle>>,
}

impl RammuxStream {
    /// Returns the ID of this stream.
    pub fn id(&self) -> StreamId {
        self.id
    }
}

impl Stream for RammuxStream {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(node) = &this.node else {
            return Poll::Ready(None);
        };

        let data = std::task::ready!(node.modify(|handle| handle.inbound.poll_read(cx.waker())));
        if data.is_none() {
            this.node = None;
        }
        Poll::Ready(data.map(From::from))
    }
}

impl Drop for RammuxStream {
    fn drop(&mut self) {
        let Some(node) = self.node.take() else {
            return;
        };
        node.modify(|handle| handle.inbound.close_reading());
    }
}
