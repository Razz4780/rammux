use std::{
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
use futures::{FutureExt, StreamExt};
use hyper::{Response, upgrade::Upgraded};
use hyper_util::rt::TokioIo;

use crate::{
    config::H2MuxerConfig,
    server::{EchoImpl, pipe::PipeBytes},
    stream_util::H2Duplex,
};

pub struct H2Echo;

impl EchoImpl for H2Echo {
    type Config = H2MuxerConfig;

    fn protocol_name() -> &'static str {
        "h2"
    }

    async fn run_on(conn: Upgraded, config: Self::Config) -> anyhow::Result<()> {
        let connection = config
            .to_server_builder()
            .handshake::<_, Bytes>(TokioIo::new(conn))
            .await
            .context("HTTP/2 handshake failed")?;
        let mut selector: Selector<H2Task, ()> = Selector::default();
        selector.push(H2Task::Connection(Box::new(connection)));
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

type H2Connection = h2::server::Connection<TokioIo<Upgraded>, Bytes>;

enum H2Task {
    /// Boxed: a connection dwarfs a pipe, and there is exactly one of it.
    Connection(Box<H2Connection>),
    Stream(PipeBytes<H2Duplex>),
}

impl Task for H2Task {
    type Cont = H2Duplex;
    type Break = anyhow::Result<()>;
    type Output = ControlFlow<anyhow::Result<()>, Self::Cont>;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match self.get_mut() {
            // Accepting drives the whole connection, the open streams'
            // frames included, so this is the only place it is polled.
            Self::Connection(connection) => match std::task::ready!(connection.poll_accept(cx)) {
                None => Poll::Ready(ControlFlow::Break(Ok(()))),
                Some(Err(error)) => Poll::Ready(ControlFlow::Break(Err(anyhow::Error::new(error)))),
                Some(Ok((request, mut respond))) => {
                    // Headers go out now; the body follows as the echo
                    // produces it, so this is not the end of the stream.
                    match respond.send_response(Response::new(()), false) {
                        Ok(send) => Poll::Ready(ControlFlow::Continue(H2Duplex::accepted(
                            request.into_body(),
                            send,
                        ))),
                        Err(error) => {
                            Poll::Ready(ControlFlow::Break(Err(anyhow::Error::new(error))))
                        },
                    }
                },
            },
            Self::Stream(pipe) => pipe
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
            // We wait for the result from the connection.
            Self::Stream(..) => None,
        }
    }
}
