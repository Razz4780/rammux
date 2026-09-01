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
use futures::{FutureExt, StreamExt};
use hyper::{HeaderMap, upgrade::Upgraded};
use hyper_util::rt::TokioIo;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode, Stream};

use crate::server::{EchoImpl, http_util, pipe::PipeFuturesIo};

pub struct YamuxEcho {
    config: Config,
}

impl EchoImpl for YamuxEcho {
    fn protocol_name() -> &'static str {
        "yamux"
    }

    fn build_from(headers: &HeaderMap) -> anyhow::Result<Self> {
        let mut config = Config::default();
        config.set_read_after_close(true);
        config.set_split_send_size(http_util::prop_from_header(headers, "frame-limit")?);
        config.set_max_num_streams(http_util::prop_from_header(headers, "max-streams")?);
        config.set_max_connection_receive_window(Some(http_util::prop_from_header(
            headers,
            "max-conn-receive-window",
        )?));
        Ok(Self { config })
    }

    async fn run_on(self, conn: Upgraded) -> anyhow::Result<()> {
        let connection = Connection::new(TokioIo::new(conn).compat(), self.config, Mode::Server);
        let mut selector: Selector<YamuxTask, ()> = Selector::default();
        selector.push(YamuxTask::Connection(connection));
        loop {
            match selector.next().await.unwrap() {
                ControlFlow::Continue(stream) => {
                    selector.push(YamuxTask::Stream(PipeFuturesIo::new(stream)));
                },
                ControlFlow::Break(result) => {
                    break result.context("yamux failed");
                },
            }
        }
    }
}

enum YamuxTask {
    Connection(Connection<Compat<TokioIo<Upgraded>>>),
    Stream(PipeFuturesIo<Stream>),
}

impl Task for YamuxTask {
    type Cont = Stream;
    type Break = anyhow::Result<()>;
    type Output = ControlFlow<anyhow::Result<()>, Self::Cont>;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        let this = self.get_mut();
        match this {
            Self::Connection(conn) => match conn.poll_next_inbound(cx) {
                Poll::Ready(None) => Poll::Ready(ControlFlow::Break(Ok(()))),
                Poll::Ready(Some(Ok(stream))) => Poll::Ready(ControlFlow::Continue(stream)),
                Poll::Ready(Some(Err(error))) => {
                    Poll::Ready(ControlFlow::Break(Err(anyhow::Error::new(error))))
                },
                Poll::Pending => Poll::Pending,
            },
            Self::Stream(stream) => stream
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
            Self::Stream(..) => None,
        }
    }
}
