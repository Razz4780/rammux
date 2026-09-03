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
    Request, Response,
    body::{Body, Incoming},
    server::conn::http2::Connection,
    service::Service,
    upgrade::Upgraded,
};
use hyper_util::rt::TokioExecutor;

use crate::{
    config::H2MuxerConfig,
    server::{EchoImpl, pipe::PipeBytes},
    stream_util::ChannelBody,
};

pub struct H2Echo;

impl EchoImpl for H2Echo {
    type Config = H2MuxerConfig;

    fn protocol_name() -> &'static str {
        "h2"
    }

    async fn run_on(conn: Upgraded, config: Self::Config) -> anyhow::Result<()> {
        let (req_tx, req_rx) = mpsc::unbounded();
        let conn = hyper::server::conn::http2::Builder::new(TokioExecutor::default())
            .auto_date_header(false)
            .keep_alive_interval(None)
            .max_frame_size(16 * 1024)
            .max_concurrent_streams(100)
            .initial_connection_window_size(config.global_recv_window)
            .initial_stream_window_size(config.stream_recv_window)
            .adaptive_window(config.adaptive_window)
            .serve_connection(
                conn,
                MultiplexerService {
                    requests_tx: req_tx,
                },
            );
        let mut selector: Selector<H2Task, ()> = Default::default();
        selector.push(H2Task::Connection(Box::new(conn)));
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
    type Response = Response<ChannelBody>;
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
        let response = Response::new(ChannelBody(outgoing_rx));
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

enum H2Task {
    Connection(Box<Connection<Upgraded, MultiplexerService, TokioExecutor>>),
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
