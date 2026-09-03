use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Context as _;
use async_selector::{
    selector::{BorrowedMut, Removed, Selector},
    task::Task,
};
use futures::{FutureExt, StreamExt};
use hyper::{HeaderMap, upgrade::Upgraded};
use hyper_util::rt::TokioIo;
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{Downgraded, RammuxConnection, RammuxProgress},
    stream::RammuxDuplex,
};

use crate::{
    rammux_rtt::RttSchedule,
    server::{EchoImpl, http_util, pipe::PipeBytes},
    stream_util::RammuxIo,
};

/// Transport under the rammux connection.
type Transport = TokioIo<Upgraded>;

pub struct RammuxEcho {
    config: RammuxConfig,
    probe_interval: Duration,
    ping_interval: Duration,
}

impl EchoImpl for RammuxEcho {
    fn protocol_name() -> &'static str {
        "rammux"
    }

    fn build_from(headers: &HeaderMap) -> anyhow::Result<Self> {
        let mut config = RammuxConfig::new();
        config.frame_limit = http_util::prop_from_header(headers, "frame-limit")?;
        config.local_recv_window = http_util::prop_from_header(headers, "local-recv-window")?;
        config.remote_recv_window = http_util::prop_from_header(headers, "remote-recv-window")?;
        config.global_recv_window = http_util::prop_from_header(headers, "global-recv-window")?;
        config.local_transit_window = http_util::prop_from_header(headers, "local-transit-window")?;
        config.remote_transit_window =
            http_util::prop_from_header(headers, "remote-transit-window")?;
        config.transit_window_max = http_util::prop_from_header(headers, "transit-window-max")?;
        config.max_inbound_streams = http_util::prop_from_header(headers, "max-streams")?;
        config.max_outbound_streams = 0;
        let probe_interval =
            http_util::prop_from_header(headers, "probe-interval").map(Duration::from_secs)?;
        let ping_interval =
            http_util::prop_from_header(headers, "ping-interval").map(Duration::from_secs)?;
        anyhow::ensure!(
            probe_interval > Duration::ZERO,
            "probe-interval must not be 0"
        );
        anyhow::ensure!(
            ping_interval > Duration::ZERO,
            "ping-interval must not be 0"
        );
        Ok(Self {
            config,
            probe_interval,
            ping_interval,
        })
    }

    async fn run_on(self, conn: Upgraded) -> anyhow::Result<()> {
        let connection = RammuxConnection::new(RammuxRole::Server, TokioIo::new(conn), self.config);
        let mut selector: Selector<RammuxTask, RttSchedule> =
            Selector::new(RttSchedule::new(self.probe_interval, self.ping_interval));
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
