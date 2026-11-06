use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::{StreamId, stream::StreamRef};

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
    sink: RammuxSink,
    stream: RammuxStream,
}

impl RammuxDuplex {
    pub(super) fn new(id: StreamId, shared: StreamRef) -> Self {
        Self {
            sink: RammuxSink {
                id,
                shared: Some(shared.clone()),
            },
            stream: RammuxStream {
                id,
                shared: Some(shared),
            },
        }
    }

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
    id: StreamId,
    /// [`None`] once writing is closed.
    shared: Option<StreamRef>,
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
        let shared = this.shared.as_ref().ok_or(io::ErrorKind::BrokenPipe)?;
        let result = std::task::ready!(shared.lock().outbound.poll_write_ready(cx.waker()));
        if result.is_err() {
            this.shared = None;
        }
        Poll::Ready(result)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let shared = this.shared.as_ref().ok_or(io::ErrorKind::BrokenPipe)?;
        let result = shared.lock().outbound.write(item, &shared.updates);
        // Only a closed direction is forgotten. A write that came before the sink was ready is rejected,
        // and the stream stays: dropping the sink must still close it.
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
        {
            this.shared = None;
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Some(shared) = &self.shared else {
            return Poll::Ready(Ok(()));
        };
        shared.lock().outbound.poll_flushed(cx.waker()).map(Ok)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let Some(shared) = &this.shared else {
            return Poll::Ready(Ok(()));
        };
        {
            let mut state = shared.lock();
            state.outbound.close_writing(&shared.updates);
            std::task::ready!(state.outbound.poll_flushed(cx.waker()));
        }
        this.shared = None;
        Poll::Ready(Ok(()))
    }
}

impl Drop for RammuxSink {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take()
            && let Some(mut state) = shared.try_lock()
        {
            state.outbound.close_writing(&shared.updates);
        }
    }
}

/// [`Stream`] half of a [`RammuxDuplex`].
///
/// Dropping it will close reading on the stream.
pub struct RammuxStream {
    id: StreamId,
    /// [`None`] once reading is closed.
    shared: Option<StreamRef>,
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
        let Some(shared) = &this.shared else {
            return Poll::Ready(None);
        };
        let data = std::task::ready!(shared.lock().inbound.poll_read(cx.waker(), &shared.updates));
        if data.is_none() {
            this.shared = None;
        }
        Poll::Ready(data.map(Bytes::from))
    }
}

impl Drop for RammuxStream {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take()
            && let Some(mut state) = shared.try_lock()
        {
            state.inbound.close_reading(&shared.updates);
        }
    }
}
