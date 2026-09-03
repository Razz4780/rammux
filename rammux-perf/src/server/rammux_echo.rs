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
use rammux::{
    config::RammuxRole,
    connection::{Downgraded, RammuxConnection, RammuxProgress},
    stream::RammuxDuplex,
};

use crate::{
    config::RammuxMuxerConfig,
    rammux_rtt::RttSchedule,
    server::{EchoImpl, pipe::PipeBytes},
    stream_util::RammuxIo,
};

/// Transport under the rammux connection.
type Transport = TokioIo<Upgraded>;

pub struct RammuxEcho;

impl EchoImpl for RammuxEcho {
    type Config = RammuxMuxerConfig;

    fn protocol_name() -> &'static str {
        "rammux"
    }

    async fn run_on(conn: Upgraded, config: Self::Config) -> anyhow::Result<()> {
        let connection = RammuxConnection::new(
            RammuxRole::Server,
            TokioIo::new(conn),
            config.to_rammux_config(),
        );
        let mut selector: Selector<RammuxTask, RttSchedule> = Selector::new(RttSchedule::new(
            config.probe_interval(),
            config.ping_interval(),
        ));
        selector.push(RammuxTask::Connection(connection));
        let downgraded = loop {
            match selector.next().await.unwrap() {
                ControlFlow::Continue(duplex) => {
                    selector.push(RammuxTask::Stream(PipeBytes::new(RammuxIo(duplex))));
                },
                ControlFlow::Break(result) => {
                    break result.context("rammux failed")?;
                },
            }
        };
        downgraded.with_shutdown().await.context("rammux failed")?;
        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
enum RammuxTask {
    Connection(RammuxConnection<Transport>),
    Stream(PipeBytes<RammuxIo>),
}

impl Task<RttSchedule> for RammuxTask {
    type Cont = RammuxDuplex;
    type Break = anyhow::Result<Option<Downgraded<Transport>>>;
    type Output = ControlFlow<anyhow::Result<Downgraded<Transport>>, RammuxDuplex>;

    fn poll_progress(
        self: Pin<&mut Self>,
        schedule: &mut RttSchedule,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match self.get_mut() {
            Self::Connection(connection) => {
                loop {
                    match schedule.poll_next(cx) {
                        Poll::Ready(Ok(due)) => {
                            if let Err(error) = schedule.apply(due, connection) {
                                return Poll::Ready(ControlFlow::Break(Err(anyhow::Error::new(
                                    error,
                                ))));
                            }
                        },
                        // An unfinished probe pauses data output on both sides,
                        // so this is the connection's liveness check.
                        Poll::Ready(Err(timeout)) => {
                            return Poll::Ready(ControlFlow::Break(Err(anyhow::Error::new(
                                timeout,
                            ))));
                        },
                        Poll::Pending => break,
                    }
                }

                loop {
                    match std::task::ready!(connection.poll_progress(cx)) {
                        Ok(RammuxProgress::Inbound(duplex)) => {
                            break Poll::Ready(ControlFlow::Continue(duplex));
                        },
                        Ok(RammuxProgress::Probe(event)) => schedule.observe(event),
                        Ok(RammuxProgress::Empty) => {},
                        Ok(RammuxProgress::Downgraded(downgraded)) => {
                            break Poll::Ready(ControlFlow::Break(Ok(Some(downgraded))));
                        },
                        Err(error) => {
                            break Poll::Ready(ControlFlow::Break(Err(anyhow::Error::new(error))));
                        },
                    }
                }
            },
            Self::Stream(stream) => stream
                .poll_unpin(cx)
                .map_err(anyhow::Error::new)
                .map_ok(|_| None)
                .map(ControlFlow::Break),
        }
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut RttSchedule,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(ControlFlow::Continue(value))
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut RttSchedule,
        value: Self::Break,
    ) -> Option<Self::Output> {
        match value {
            Ok(Some(downgraded)) => Some(ControlFlow::Break(Ok(downgraded))),
            Ok(None) => None,
            Err(error) => Some(ControlFlow::Break(Err(error))),
        }
    }
}
