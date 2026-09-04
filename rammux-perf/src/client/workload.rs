//! One iteration: the streams, what each measures, and the loop that runs them.
//!
//! Everything runs in one task on one [`Selector`], like the server side does.
//! The connection is driven by one task, the streams are one task each, and
//! the loop that owns the selector turns their reports into statistics.

use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::{
    selector::{BorrowedMut, Removed, Selector},
    task::Task,
};
use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::time::Instant;

use crate::client::muxer::Muxer;

const CHUNK: Bytes = Bytes::from_static(&[b'X'; 256 * 1024]);

/// What one iteration runs.
pub struct Workload {
    /// How many bulk streams to open.
    pub bulk_streams: usize,
    /// How much data each bulk stream sends, and reads back, in bytes.
    pub bulk_stream_data: usize,
    /// Size of the ping pong message, if a ping pong stream runs.
    pub ping_pong_size: Option<usize>,
}

/// What one iteration measured.
///
/// Raw samples, one entry per stream and per exchange, rather than anything
/// summarised: a run is many iterations, and percentiles have to be taken
/// over all of their samples at once. A percentile of per-iteration
/// percentiles is not a percentile of anything.
#[derive(Default)]
pub struct Measurements {
    /// How long each bulk stream took to send its data and read the echo back.
    pub bulk_elapsed: Vec<Duration>,
    /// Round trip of each completed ping pong exchange.
    pub latencies: Vec<Duration>,
}

/// Runs the workload on `muxer` and closes the connection.
///
/// The iteration ends when every bulk stream has read its echo back. A ping
/// pong exchange still in flight is abandoned: its stream is closed and
/// drained rather than dropped, so the server is never mid-echo on a
/// connection that is going away, but the exchange is not counted.
pub async fn run<M: Muxer>(muxer: M, workload: &Workload) -> anyhow::Result<Measurements> {
    let streams = workload.bulk_streams + usize::from(workload.ping_pong_size.is_some());
    let mut selector: Selector<IterationTask<M>, Phase> = Selector::new(Phase::Running);
    selector.push(IterationTask::Driver(Driver {
        muxer,
        to_open: streams,
    }));

    let mut measurements = Measurements::default();
    let mut remaining_bulk = workload.bulk_streams;
    // The ping pong stream goes first, so its samples cover the whole bulk phase.
    let mut ping_pong_pending = workload.ping_pong_size;
    let mut ping_pong_running = workload.ping_pong_size.is_some();

    loop {
        let event = selector
            .next()
            .await
            .expect("the driver breaks before the selector empties")?;
        match event {
            Event::Opened(stream) => match ping_pong_pending.take() {
                Some(size) => {
                    selector.push(IterationTask::PingPong(PingPong::new(stream, size)));
                },
                None => {
                    selector.push(IterationTask::Bulk(Bulk::new(
                        stream,
                        workload.bulk_stream_data,
                    )));
                },
            },
            Event::Latency(latency) => measurements.latencies.push(latency),
            Event::BulkDone { elapsed } => {
                measurements.bulk_elapsed.push(elapsed);
                remaining_bulk -= 1;
                if remaining_bulk == 0 {
                    // Tasks are parked on the connection's wakers, none of
                    // which fire for a change of phase.
                    *selector.strategy_mut() = if ping_pong_running {
                        Phase::Draining
                    } else {
                        Phase::Closing
                    };
                    selector.wake_all();
                }
            },
            Event::PingPongDone => {
                ping_pong_running = false;
                *selector.strategy_mut() = Phase::Closing;
                selector.wake_all();
            },
            Event::Closed => {
                anyhow::ensure!(
                    remaining_bulk == 0,
                    "the connection closed with {remaining_bulk} bulk streams unfinished"
                );
                return Ok(measurements);
            },
        }
    }
}

/// What the tasks are doing, shared with all of them as the selector's strategy.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Streams are being opened and run.
    Running,
    /// Every bulk stream is done: the ping pong stream winds down.
    Draining,
    /// Nothing is left on the connection: the driver closes it.
    Closing,
}

/// What a task reports to the loop.
enum Event<S> {
    /// The driver opened a stream.
    Opened(S),
    /// The ping pong stream completed an exchange.
    Latency(Duration),
    /// A bulk stream read its echo back.
    BulkDone { elapsed: Duration },
    /// The ping pong stream is closed and drained.
    PingPongDone,
    /// The connection is closed.
    Closed,
}

/// What a task's poll produces: an event to continue with, or a final one.
type Progress<S> = Poll<ControlFlow<anyhow::Result<Event<S>>, Event<S>>>;

enum IterationTask<M: Muxer> {
    Driver(Driver<M>),
    Bulk(Bulk<M::Stream>),
    PingPong(PingPong<M::Stream>),
}

impl<M: Muxer> Task<Phase> for IterationTask<M> {
    type Cont = Event<M::Stream>;
    type Break = anyhow::Result<Event<M::Stream>>;
    type Output = anyhow::Result<Event<M::Stream>>;

    fn poll_progress(
        self: Pin<&mut Self>,
        phase: &mut Phase,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match self.get_mut() {
            Self::Driver(driver) => driver.poll(*phase, cx),
            Self::Bulk(bulk) => bulk.poll(cx).map(ControlFlow::Break),
            Self::PingPong(ping_pong) => ping_pong.poll(*phase, cx),
        }
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut Phase,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(Ok(value))
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut Phase,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some(value)
    }
}

/// Drives the connection, and opens the streams the iteration needs.
struct Driver<M> {
    muxer: M,
    /// Streams still to be opened.
    to_open: usize,
}

impl<M: Muxer> Driver<M> {
    fn poll(&mut self, phase: Phase, cx: &mut Context<'_>) -> Progress<M::Stream> {
        if phase == Phase::Closing {
            return self
                .muxer
                .poll_close(cx)
                .map(|result| ControlFlow::Break(result.map(|()| Event::Closed)));
        }

        // Drive first: it registers the connection's wakers, and a connection
        // that ended is the end of the iteration whatever else is pending.
        match self.muxer.poll_drive(cx) {
            Poll::Ready(Ok(())) => {
                return Poll::Ready(ControlFlow::Break(Err(anyhow::anyhow!(
                    "the server closed the connection"
                ))));
            },
            Poll::Ready(Err(error)) => return Poll::Ready(ControlFlow::Break(Err(error))),
            Poll::Pending => {},
        }

        if self.to_open > 0 {
            match self.muxer.poll_open(cx) {
                Poll::Ready(Ok(stream)) => {
                    self.to_open -= 1;
                    return Poll::Ready(ControlFlow::Continue(Event::Opened(stream)));
                },
                Poll::Ready(Err(error)) => return Poll::Ready(ControlFlow::Break(Err(error))),
                Poll::Pending => {},
            }
        }

        Poll::Pending
    }
}

/// Sends a fixed amount of data as fast as the stream takes it, and reads
/// the echo back. Done when the echo is complete.
struct Bulk<S> {
    stream: S,
    /// Bytes still to send.
    to_send: usize,
    /// Whether the sink has been closed after the last byte.
    sink_closed: bool,
    /// Bytes of echo read so far.
    received: usize,
    /// Bytes to send in total, and to read back.
    total: usize,
    started: Instant,
}

impl<S: MuxerStream> Bulk<S> {
    fn new(stream: S, total: usize) -> Self {
        Self {
            stream,
            to_send: total,
            sink_closed: false,
            received: 0,
            total,
            started: Instant::now(),
        }
    }

    fn poll<E>(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Event<E>>> {
        loop {
            let mut progressed = false;

            // Send side: hand over chunks for as long as the sink takes them,
            // then close it so the server learns the stream is complete.
            if self.to_send > 0 {
                if let Poll::Ready(result) = self.stream.poll_ready_unpin(cx) {
                    result?;
                    let len = self.to_send.min(CHUNK.len());
                    self.stream.start_send_unpin(CHUNK.slice(..len))?;
                    self.to_send -= len;
                    progressed = true;
                }
            } else if !self.sink_closed
                && let Poll::Ready(result) = self.stream.poll_close_unpin(cx)
            {
                result?;
                self.sink_closed = true;
                progressed = true;
            }

            // Receive side.
            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(data)) => {
                    self.received += data?.len();
                    progressed = true;
                },
                Poll::Ready(None) => {
                    if self.received != self.total {
                        return Poll::Ready(Err(anyhow::anyhow!(
                            "bulk stream echoed {} of {} bytes",
                            self.received,
                            self.total,
                        )));
                    }
                    return Poll::Ready(Ok(Event::BulkDone {
                        elapsed: self.started.elapsed(),
                    }));
                },
                Poll::Pending => {},
            }

            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

/// Sends one message, waits for its echo, repeats. Reports each round trip.
struct PingPong<S> {
    stream: S,
    message: Bytes,
    state: PingPongState,
}

enum PingPongState {
    /// The message is to be handed to the sink.
    Send,
    /// The message is in the sink, waiting to be flushed to the connection.
    Flush { sent_at: Instant },
    /// The message is out; its echo is being read.
    Await { sent_at: Instant, received: usize },
    /// The iteration is over: close the sink, read until the server closes
    /// its side, and report nothing more.
    Drain { sink_closed: bool },
}

impl<S: MuxerStream> PingPong<S> {
    fn new(stream: S, size: usize) -> Self {
        Self {
            stream,
            message: CHUNK.slice(..size),
            state: PingPongState::Send,
        }
    }

    fn poll<E>(&mut self, phase: Phase, cx: &mut Context<'_>) -> Progress<E> {
        if phase != Phase::Running && !matches!(self.state, PingPongState::Drain { .. }) {
            self.state = PingPongState::Drain { sink_closed: false };
        }
        loop {
            match self.state {
                PingPongState::Drain { sink_closed } => {
                    if !sink_closed {
                        if let Err(error) = std::task::ready!(self.stream.poll_close_unpin(cx)) {
                            return Poll::Ready(ControlFlow::Break(Err(error.into())));
                        }
                        self.state = PingPongState::Drain { sink_closed: true };
                    }
                    return match std::task::ready!(self.stream.poll_next_unpin(cx)) {
                        Some(Ok(..)) => continue,
                        Some(Err(error)) => Poll::Ready(ControlFlow::Break(Err(error.into()))),
                        None => Poll::Ready(ControlFlow::Break(Ok(Event::PingPongDone))),
                    };
                },
                PingPongState::Send => {
                    if let Err(error) = std::task::ready!(self.stream.poll_ready_unpin(cx)) {
                        return Poll::Ready(ControlFlow::Break(Err(error.into())));
                    }
                    // The clock starts when the application hands the message
                    // over: whatever the multiplexer does with it from here is
                    // part of the latency the application sees.
                    let sent_at = Instant::now();
                    if let Err(error) = self.stream.start_send_unpin(self.message.clone()) {
                        return Poll::Ready(ControlFlow::Break(Err(error.into())));
                    }
                    self.state = PingPongState::Flush { sent_at };
                },
                PingPongState::Flush { sent_at } => {
                    if let Err(error) = std::task::ready!(self.stream.poll_flush_unpin(cx)) {
                        return Poll::Ready(ControlFlow::Break(Err(error.into())));
                    }
                    self.state = PingPongState::Await {
                        sent_at,
                        received: 0,
                    };
                },
                PingPongState::Await { sent_at, received } => {
                    match std::task::ready!(self.stream.poll_next_unpin(cx)) {
                        Some(Ok(data)) => {
                            let received = received + data.len();
                            if received > self.message.len() {
                                return Poll::Ready(ControlFlow::Break(Err(anyhow::anyhow!(
                                    "ping pong echo ran {} bytes over the message",
                                    received - self.message.len(),
                                ))));
                            }
                            if received == self.message.len() {
                                self.state = PingPongState::Send;
                                return Poll::Ready(ControlFlow::Continue(Event::Latency(
                                    sent_at.elapsed(),
                                )));
                            }
                            self.state = PingPongState::Await { sent_at, received };
                        },
                        Some(Err(error)) => {
                            return Poll::Ready(ControlFlow::Break(Err(error.into())));
                        },
                        None => {
                            return Poll::Ready(ControlFlow::Break(Err(anyhow::anyhow!(
                                "the server closed the ping pong stream"
                            ))));
                        },
                    }
                },
            }
        }
    }
}

/// Shorthand for the stream bounds that [`Muxer::Stream`] carries.
trait MuxerStream:
    Sink<Bytes, Error = std::io::Error> + Stream<Item = std::io::Result<Bytes>> + Unpin
{
}

impl<S> MuxerStream for S where
    S: Sink<Bytes, Error = std::io::Error> + Stream<Item = std::io::Result<Bytes>> + Unpin
{
}
