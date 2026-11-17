use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::{StreamId, error::StreamError, stream::StreamRef};

/// Bidirectional virtual data stream created within a rammux connection.
///
/// It is a [`RammuxSink`] and a [`RammuxStream`] in one, see them for how the stream ends.
/// [`into_split`](Self::into_split) separates them.
///
/// Dropping it drops both halves: the peer reads [`StreamError::PeerAborted`] after the data it got,
/// unless the sink was closed first, and the peer's writes fail with [`StreamError::PeerStoppedReading`].
/// [`reset`](Self::reset) gives up on the stream without making the peer read that data first.
pub struct RammuxDuplex {
    sink: RammuxSink,
    stream: RammuxStream,
}

impl RammuxDuplex {
    pub(super) fn new(id: StreamId, shared: StreamRef) -> Self {
        Self {
            sink: RammuxSink {
                id,
                shared: shared.clone(),
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

    /// Gives up on the stream in both directions: [resets](RammuxSink::reset) the sink,
    /// and drops the [`RammuxStream`].
    pub fn reset(self) {
        let Self { sink, stream } = self;
        sink.reset();
        drop(stream);
    }
}

impl fmt::Debug for RammuxDuplex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RammuxDuplex")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl Stream for RammuxDuplex {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx)
    }
}

impl Sink<Bytes> for RammuxDuplex {
    type Error = StreamError;

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
/// # Ending the stream
///
/// Only [`Sink::poll_close`] (`close()` of [`SinkExt`]) ends the stream cleanly: the data that was written goes
/// out, and then the peer's [`RammuxStream`] ends with [`None`]. A sink that is dropped after `close()` was
/// called finishes the close, even if `close()` did not complete.
///
/// Dropping the sink before `close()` was called aborts the stream: the data that the connection has not
/// taken yet is discarded, and the peer's [`RammuxStream`] yields [`StreamError::PeerAborted`]
/// after the data that it got.
///
/// [`reset`](Self::reset) gives up on the stream. Use it when the data is not worth delivering any more,
/// for example when the peer's reader stalls and its unread data holds buffers that other streams need.
///
/// # Errors
///
/// Once writing ended for a reason other than `close()`, every call returns the reason, including
/// [`Sink::poll_flush`] and [`Sink::poll_close`]:
/// - [`StreamError::PeerStoppedReading`] when the peer dropped its [`RammuxStream`],
/// - [`StreamError::ConnectionFailed`], [`StreamError::ConnectionDowngraded`] or
///   [`StreamError::ConnectionDropped`] when the connection ended. A sink that completed its `close()`
///   before that stays closed.
///
/// After `close()` completed, writing returns [`StreamError::SinkClosed`], and flushing and closing
/// return `Ok`. [`Sink::start_send`] before [`Sink::poll_ready`] returned [`Poll::Ready`] returns
/// [`StreamError::SinkNotReady`], and the sink can still be used.
///
/// # Flushing
///
/// [`Sink::poll_flush`] and [`Sink::poll_close`] return `Ok` as soon as all pending data
/// is framed and enqueued for sending through the IO transport, and `poll_close` once the end of the stream
/// is enqueued too. So a downgrade that this side starts right after `close()` completed does not cut
/// the stream short. It does not mean that the peer received the data: data that is still on its way when
/// the connection fails, or when the peer starts a downgrade, is lost.
pub struct RammuxSink {
    id: StreamId,
    shared: StreamRef,
}

impl RammuxSink {
    /// Returns the ID of this stream.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// Resets the stream: the data that the connection has not taken yet is discarded, and so is the data
    /// that the peer's [`RammuxStream`] has not yielded yet, once the peer learns about the reset.
    /// The peer's [`RammuxStream`] then yields [`StreamError::PeerReset`], and the peer's buffers
    /// that the stream used are free for other streams.
    ///
    /// A sink whose `close()` still waits for its data to go out is reset too. The reset does nothing once
    /// the connection has taken the end of the stream (`close()` completed, or completes when it is polled
    /// next), or if writing ended for another reason: the stream keeps the end it got.
    pub fn reset(self) {
        if let Some(mut state) = self.shared.try_lock() {
            state.outbound.reset_writing(&self.shared.data_task);
        }
    }
}

impl fmt::Debug for RammuxSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RammuxSink")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Sink<Bytes> for RammuxSink {
    type Error = StreamError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.shared.lock().outbound.poll_write_ready(cx.waker())
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let shared = &self.shared;
        shared.lock().outbound.write(item, &shared.data_task)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.shared.lock().outbound.poll_flushed(cx.waker())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let shared = &self.shared;
        let mut state = shared.lock();
        state.outbound.close_writing(&shared.data_task);
        state.outbound.poll_flushed(cx.waker())
    }
}

impl Drop for RammuxSink {
    fn drop(&mut self) {
        if let Some(mut state) = self.shared.try_lock() {
            state.outbound.drop_writing(&self.shared.data_task);
        }
    }
}

/// [`Stream`] half of a [`RammuxDuplex`].
///
/// It yields the data in the order the peer wrote it. Once the peer closed its [`RammuxSink`], the data is
/// followed by [`None`]. If the stream ended in another way, the data that arrived is followed by one error
/// that names the reason, and then by [`None`]:
/// - [`StreamError::PeerAborted`] when the peer dropped its sink without closing it,
/// - [`StreamError::PeerReset`] when the peer reset the stream. This one does not wait for the data:
///   what was not yielded before the reset arrived is discarded,
/// - [`StreamError::ConnectionFailed`], [`StreamError::ConnectionDowngraded`] or
///   [`StreamError::ConnectionDropped`] when the connection ended before the peer closed its sink.
///   A stream that the peer closed before that still ends with [`None`].
///
/// Dropping it stops reading: the data that arrives later is discarded, and writing on the peer's side fails
/// with [`StreamError::PeerStoppedReading`].
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

impl fmt::Debug for RammuxStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RammuxStream")
            .field("id", &self.id)
            .field("closed", &self.shared.is_none())
            .finish_non_exhaustive()
    }
}

impl Stream for RammuxStream {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(shared) = &this.shared else {
            return Poll::Ready(None);
        };
        let item = std::task::ready!(
            shared
                .lock()
                .inbound
                .poll_read(cx.waker(), &shared.credit_task)
        );
        if item.is_none() {
            this.shared = None;
        }
        Poll::Ready(item.map(|result| result.map(Bytes::from)))
    }
}

impl Drop for RammuxStream {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take()
            && let Some(mut state) = shared.try_lock()
        {
            state.inbound.close_reading(&shared.credit_task);
        }
    }
}
