use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::{StreamId, stream::SharedStreamState};

/// Bidirectional virtual data stream created within a rammux connection.
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
    pub(super) state: Option<Arc<Mutex<SharedStreamState>>>,
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
        let state = this.state.as_ref().ok_or(io::ErrorKind::BrokenPipe)?;
        let result = std::task::ready!(state.lock().unwrap().outbound.poll_write_ready(cx.waker()));
        if result.is_err() {
            this.state = None;
        }
        Poll::Ready(result)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let state = this.state.as_ref().ok_or(io::ErrorKind::BrokenPipe)?;
        let mut guard = state.lock().unwrap();
        let SharedStreamState {
            outbound,
            updates_poller,
            ..
        } = &mut *guard;
        let result = outbound.write(item, updates_poller);
        drop(guard);
        if result.is_err() {
            this.state = None;
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .state
            .as_ref()
            .map(|state| state.lock().unwrap().outbound.poll_flushed(cx.waker()))
            .unwrap_or(Poll::Ready(()))
            .map(Ok)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let Some(state) = &this.state else {
            return Poll::Ready(Ok(()));
        };
        let mut guard = state.lock().unwrap();
        let SharedStreamState {
            outbound,
            updates_poller,
            ..
        } = &mut *guard;
        outbound.close_writing(updates_poller);
        std::task::ready!(outbound.poll_flushed(cx.waker()));
        drop(guard);
        this.state = None;
        Poll::Ready(Ok(()))
    }
}

impl Drop for RammuxSink {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let Ok(mut guard) = state.lock() else {
            return;
        };
        let SharedStreamState {
            outbound,
            updates_poller,
            ..
        } = &mut *guard;
        outbound.close_writing(updates_poller);
    }
}

/// [`Stream`] half of a [`RammuxDuplex`].
///
/// Dropping it will close reading on the stream.
pub struct RammuxStream {
    pub(super) id: StreamId,
    pub(super) state: Option<Arc<Mutex<SharedStreamState>>>,
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
        let Some(state) = &this.state else {
            return Poll::Ready(None);
        };

        let mut guard = state.lock().unwrap();
        let SharedStreamState {
            inbound,
            updates_poller,
            ..
        } = &mut *guard;
        let data = std::task::ready!(inbound.poll_read(cx.waker(), updates_poller));
        drop(guard);
        if data.is_none() {
            this.state = None;
        }
        Poll::Ready(data.map(From::from))
    }
}

impl Drop for RammuxStream {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let Ok(mut guard) = state.lock() else {
            return;
        };
        let SharedStreamState {
            inbound,
            updates_poller,
            ..
        } = &mut *guard;
        inbound.close_reading(updates_poller);
    }
}
