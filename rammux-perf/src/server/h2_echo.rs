use std::{
    future::Ready,
    io,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Context as _;
use async_selector::{
    selector::{BorrowedMut, Removed, Selector},
    task::Task,
};
use bytes::Bytes;
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt, channel::mpsc};
use hyper::{
    HeaderMap, Request, Response,
    body::{Body, Frame, Incoming},
    server::conn::http2::Connection,
    service::Service,
    upgrade::Upgraded,
};
use hyper_util::rt::TokioExecutor;

use crate::server::{EchoImpl, http_util, pipe::PipeBytes};

pub struct H2Echo {
    adaptive_window: bool,
    max_frame_size: u32,
    max_concurrent_streams: u32,
    max_send_buf_size: usize,
    initial_stream_window: u32,
    initial_connection_window: u32,
}

impl EchoImpl for H2Echo {
    fn protocol_name() -> &'static str {
        "h2"
    }

    fn build_from(headers: &HeaderMap) -> anyhow::Result<Self> {
        Ok(Self {
            adaptive_window: http_util::prop_from_header(headers, "adaptive-window")?,
            max_frame_size: http_util::prop_from_header(headers, "frame-limit")?,
            max_concurrent_streams: http_util::prop_from_header(headers, "max-streams")?,
            max_send_buf_size: http_util::prop_from_header(headers, "max-send-buf-size")?,
            initial_stream_window: http_util::prop_from_header(headers, "initial-stream-window")?,
            initial_connection_window: http_util::prop_from_header(
                headers,
                "initial-connection-window",
            )?,
        })
    }

    async fn run_on(self, conn: Upgraded) -> anyhow::Result<()> {
        let (req_tx, req_rx) = mpsc::unbounded();
        let conn = hyper::server::conn::http2::Builder::new(TokioExecutor::default())
            .auto_date_header(false)
            .keep_alive_interval(None)
            .adaptive_window(self.adaptive_window)
            .max_concurrent_streams(self.max_concurrent_streams)
            .max_frame_size(self.max_frame_size)
            .max_send_buf_size(self.max_send_buf_size)
            .initial_connection_window_size(self.initial_connection_window)
            .initial_stream_window_size(self.initial_stream_window)
            .serve_connection(
                conn,
                MultiplexerService {
                    requests_tx: req_tx,
                },
            );
        let mut selector: Selector<H2Task, ()> = Default::default();
        selector.push(H2Task::Connection(conn));
        selector.push(H2Task::NewStreams(req_rx));
        loop {
            match selector.next().await.unwrap() {
                ControlFlow::Continue(stream) => {
                    selector.push(H2Task::Stream(PipeBytes::new(stream)));
                },
                ControlFlow::Break(result) => {
                    break result.context("h2 failed");
                },
            }
        }
    }
}

/// HTTP/2 [`Service`] that sends requests' and responses' bodies through an internal channel,
/// feeding them back to the main echo loop.
struct MultiplexerService {
    requests_tx: mpsc::UnboundedSender<EmulatedStream>,
}

impl Service<Request<Incoming>> for MultiplexerService {
    type Response = Response<ResponseBody>;
    type Error = hyper::Error;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let incoming = req.into_body();
        let (outgoing_tx, outgoing_rx) = mpsc::channel(1);
        let stream = EmulatedStream {
            incoming,
            outgoing: outgoing_tx,
        };
        let _ = self.requests_tx.unbounded_send(stream);
        let response = Response::new(ResponseBody(outgoing_rx));
        std::future::ready(Ok(response))
    }
}

/// Logical stream in a multiplexed echo.
///
/// Emulated using [`Response`] and [`Request`] bodies.
struct EmulatedStream {
    incoming: Incoming,
    outgoing: mpsc::Sender<Bytes>,
}

impl Sink<Bytes> for EmulatedStream {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .outgoing
            .poll_ready_unpin(cx)
            .map_err(io::Error::other)
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.get_mut()
            .outgoing
            .start_send_unpin(item)
            .map_err(io::Error::other)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .outgoing
            .poll_flush_unpin(cx)
            .map_err(io::Error::other)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .outgoing
            .poll_close_unpin(cx)
            .map_err(io::Error::other)
    }
}

impl Stream for EmulatedStream {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let result = std::task::ready!(Pin::new(&mut this.incoming).poll_frame(cx));
            match result {
                None => break Poll::Ready(None),
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        break Poll::Ready(Some(Ok(data)));
                    }
                },
                Some(Err(error)) => break Poll::Ready(Some(Err(io::Error::other(error)))),
            }
        }
    }
}

struct ResponseBody(mpsc::Receiver<Bytes>);

impl Body for ResponseBody {
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

enum H2Task {
    Connection(Connection<Upgraded, MultiplexerService, TokioExecutor>),
    NewStreams(mpsc::UnboundedReceiver<EmulatedStream>),
    Stream(PipeBytes<EmulatedStream>),
}

impl Task for H2Task {
    type Cont = EmulatedStream;
    type Break = anyhow::Result<()>;
    type Output = ControlFlow<anyhow::Result<()>, EmulatedStream>;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match self.get_mut() {
            Self::Connection(connection) => connection
                .poll_unpin(cx)
                .map_err(anyhow::Error::new)
                .map(ControlFlow::Break),
            Self::NewStreams(unbounded_receiver) => {
                match std::task::ready!(unbounded_receiver.poll_next_unpin(cx)) {
                    Some(stream) => Poll::Ready(ControlFlow::Continue(stream)),
                    None => Poll::Ready(ControlFlow::Break(Ok(()))),
                }
            },
            Self::Stream(pipe_bytes) => pipe_bytes
                .poll_unpin(cx)
                .map_err(anyhow::Error::new)
                .map(ControlFlow::Break),
        }
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut (),
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(ControlFlow::Continue(value))
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut (),
        value: Self::Break,
    ) -> Option<Self::Output> {
        match task.into_inner() {
            Self::Connection(..) => Some(ControlFlow::Break(value)),
            // we wait for the result from the connection
            Self::NewStreams(..) => None,
            Self::Stream(..) => None,
        }
    }
}
