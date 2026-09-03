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
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, Mode, Stream};

use crate::{
    config::YamuxMuxerConfig,
    server::{EchoImpl, pipe::PipeFuturesIo},
};

pub struct YamuxEcho;

impl EchoImpl for YamuxEcho {
    type Config = YamuxMuxerConfig;

    fn protocol_name() -> &'static str {
        "yamux"
    }

    async fn run_on(conn: Upgraded, config: Self::Config) -> anyhow::Result<()> {
        let mut yamux_config = Config::default();
        yamux_config
            .set_max_connection_receive_window(Some(config.max_connection_receive_window))
            .set_max_num_streams(config.max_num_streams)
            .set_split_send_size(config.split_send_size)
            .set_read_after_close(true);
        let connection = Connection::new(TokioIo::new(conn).compat(), yamux_config, Mode::Server);
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

#[allow(clippy::large_enum_variant)]
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
