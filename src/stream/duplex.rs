use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::{
    StreamId,
    error::StreamError,
    stream::{StreamRef, StreamStats},
};

/// Bidirectional virtual data stream created within a rammux connection.
///
/// It is a [`RammuxSink`] and a [`RammuxStream`] in one, see them for more info.
///
/// Dropping it closes both halves.
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
                on_close: SinkClose::default(),
            },
            stream: RammuxStream { id, shared },
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

    /// Returns where the bytes of the stream are, see [`StreamStats`].
    pub fn stats(&self) -> StreamStats {
        self.sink.stats()
    }

    /// Sets how the sink closes writing, see [`RammuxSink::on_close`].
    pub fn on_close(&mut self, close: SinkClose) {
        self.sink.on_close(close);
    }
}

impl fmt::Debug for RammuxDuplex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RammuxDuplex")
            .field("id", &self.id())
            .field("on_close", &self.sink.on_close)
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

/// How a [`RammuxSink`] closes writing, see [`RammuxSink::on_close`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SinkClose {
    /// Ends the stream cleanly.
    ///
    /// Unsent data is flushed, and then the peer's [`RammuxStream`] ends with [`None`].
    #[default]
    Clean,
    /// Aborts the stream.
    ///
    /// Unsent data is discarded. The peer's [`RammuxStream`] ends with [`StreamError::PeerAborted`],
    /// after yielding the data it has already received.
    Abort,
    /// Resets the stream.
    ///
    /// Unsent data is discarded. The peer's [`RammuxStream`] ends with [`StreamError::PeerReset`],
    /// and it might not yield the data it has already received. Discarding received data on the other side
    /// allows for freeing the peer's buffers, making them available for other streams.
    ///
    /// Use it when the data is not worth delivering any more, for example when the peer's reader stalls
    /// and its unread data holds buffers that other streams need.
    Reset,
}

/// [`Sink`] half of a [`RammuxDuplex`].
///
/// # Ending the stream
///
/// [`Sink::poll_close`] (or [`SinkExt::close`]) closes writing, and so does dropping the sink.
/// How the stream ends is up to [`on_close`](Self::on_close).
///
/// # Errors
///
/// Once writing ended for a reason other than an explicit close, every call returns the reason, including
/// [`Sink::poll_flush`] and [`Sink::poll_close`]. The reason is returned as a [`StreamError`].
///
/// After an explicit close completed, writing returns [`StreamError::SinkClosed`]. Flushing and closing
/// return `Ok`. [`Sink::start_send`] before [`Sink::poll_ready`] returned [`Poll::Ready`] returns
/// [`StreamError::SinkNotReady`], and the sink can still be used.
///
/// # Flushing
///
/// [`Sink::poll_flush`] and [`Sink::poll_close`] return `Ok` as soon as all pending data and flags
/// are framed and enqueued for sending through the IO transport. At that moment, starting
/// a connection downgrade does not cut the stream short. However, it does not mean that the peer
/// **received** the data.
pub struct RammuxSink {
    id: StreamId,
    shared: StreamRef,
    /// How [`Sink::poll_close`] and [`Drop`] close writing.
    on_close: SinkClose,
}

impl RammuxSink {
    /// Returns the ID of this stream.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// Returns where the bytes of the stream are, see [`StreamStats`].
    pub fn stats(&self) -> StreamStats {
        self.shared.lock().stats()
    }

    /// Sets how the sink closes writing, in [`Sink::poll_close`] and when it is dropped.
    ///
    /// The default is [`SinkClose::Clean`].
    ///
    /// It takes effect at the next [`Sink::poll_close`], or at the drop.
    /// A close that waits to hand its end of the stream to the connection can still turn into
    /// [`SinkClose::Abort`] or [`SinkClose::Reset`] for example when the peer's reader stalls.
    /// It cannot turn back into [`SinkClose::Clean`] the data is discarded by then.
    ///
    /// Once the end of the stream is handed to the connection, or once writing
    /// ended for another reason, this method is a noop.
    pub fn on_close(&mut self, close: SinkClose) {
        self.on_close = close;
    }
}

impl fmt::Debug for RammuxSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RammuxSink")
            .field("id", &self.id)
            .field("on_close", &self.on_close)
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
        state
            .outbound
            .close_writing(self.on_close, &shared.data_task);
        state.outbound.poll_flushed(cx.waker())
    }
}

impl Drop for RammuxSink {
    fn drop(&mut self) {
        if let Some(mut state) = self.shared.try_lock() {
            state
                .outbound
                .close_writing(self.on_close, &self.shared.data_task);
        }
    }
}

/// [`Stream`] half of a [`RammuxDuplex`].
///
/// It yields the data in the order the peer wrote it. Once the peer closed its [`RammuxSink`],
/// the data is followed by [`None`]. If the stream ended in another way, the data that arrived
/// is followed by one error that names the reason, and then by [`None`].
///
/// Dropping it stops reading: the data that arrives later is discarded, and writing on the peer's
/// side fails with [`StreamError::PeerStoppedReading`].
pub struct RammuxStream {
    id: StreamId,
    shared: StreamRef,
}

impl RammuxStream {
    /// Returns the ID of this stream.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// Returns where the bytes of the stream are, see [`StreamStats`].
    ///
    /// # Panics
    ///
    /// Panics if another user of the stream panicked while holding its lock.
    pub fn stats(&self) -> StreamStats {
        self.shared.lock().stats()
    }
}

impl fmt::Debug for RammuxStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RammuxStream")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Stream for RammuxStream {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let shared = &self.shared;
        let item = std::task::ready!(
            shared
                .lock()
                .inbound
                .poll_read(cx.waker(), &shared.credit_task)
        );
        Poll::Ready(item.map(|result| result.map(Bytes::from)))
    }
}

impl Drop for RammuxStream {
    fn drop(&mut self) {
        if let Some(mut state) = self.shared.try_lock() {
            state.inbound.close_reading(&self.shared.credit_task);
        }
    }
}
