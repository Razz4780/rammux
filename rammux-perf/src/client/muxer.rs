//! The benchmarked multiplexers behind one interface, from the client's side.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Context as _;
use bytes::{Buf, Bytes};
use futures::{AsyncWrite, FutureExt, Sink, SinkExt, Stream, StreamExt};
use h2::client::SendRequest;
use hyper::{Method, Request, upgrade::Upgraded};
use hyper_util::rt::TokioIo;
use rammux::{
    config::RammuxRole,
    connection::{Downgraded, RammuxConnection, RammuxProgress},
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::{
    config::{H2MuxerConfig, RammuxMuxerConfig, YamuxMuxerConfig},
    rammux_rtt::RttSchedule,
    stream_util::{H2Duplex, RammuxIo},
};

/// A multiplexed connection to the echo server.
///
/// The three protocols differ in how a connection is driven, how a stream is
/// opened and how the connection is closed; the workload does not care about
/// any of that.
pub trait Muxer: Unpin {
    /// One logical stream over the connection.
    type Stream: Sink<Bytes, Error = io::Error> + Stream<Item = io::Result<Bytes>> + Unpin;

    /// Opens a new outbound stream.
    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Self::Stream>>;

    /// Drives the connection: moves frames on behalf of every open stream.
    ///
    /// Resolves only when the connection is over. `Ok` means the server
    /// ended it, which is not what a client in the middle of an iteration
    /// wants either.
    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>>;

    /// Closes the connection, resolving once the close is complete.
    ///
    /// Drives the connection meanwhile; [`Muxer::poll_drive`] is not called
    /// again after the first call to this.
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>>;
}

// ---------------------------------------------------------------------------
// rammux
// ---------------------------------------------------------------------------

type RammuxTransport = TokioIo<Upgraded>;

/// A rammux connection, with the probe and ping schedule it needs driven.
pub struct RammuxMuxer {
    state: RammuxState,
    schedule: RttSchedule,
}

enum RammuxState {
    /// Boxed: a connection is several hundred bytes, the other variants a
    /// fraction of that, and there is exactly one per iteration.
    Active(Box<RammuxConnection<RammuxTransport>>),
    Downgrading(Downgraded<RammuxTransport>),
    Done,
}

impl RammuxMuxer {
    pub fn new(upgraded: Upgraded, config: &RammuxMuxerConfig) -> Self {
        Self {
            state: RammuxState::Active(Box::new(RammuxConnection::new(
                RammuxRole::Client,
                TokioIo::new(upgraded),
                config.to_rammux_config(),
            ))),
            schedule: RttSchedule::new(config.probe_interval(), config.ping_interval()),
        }
    }
}

impl Muxer for RammuxMuxer {
    type Stream = RammuxIo;

    fn poll_open(&mut self, _: &mut Context<'_>) -> Poll<anyhow::Result<Self::Stream>> {
        let RammuxState::Active(connection) = &mut self.state else {
            return Poll::Ready(Err(anyhow::anyhow!("the connection is closed")));
        };
        Poll::Ready(
            connection
                .try_start_outbound()
                .context("failed to start a stream")
                .and_then(|duplex| duplex.context("stream limit reached"))
                .map(RammuxIo),
        )
    }

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        loop {
            match &mut self.state {
                RammuxState::Active(connection) => {
                    // The schedule first: it is what starts the probes and
                    // pings the connection then has to carry, and it is the
                    // liveness check - an unfinished probe pauses data
                    // output on both sides.
                    loop {
                        match self.schedule.poll_next(cx) {
                            Poll::Ready(Ok(due)) => self
                                .schedule
                                .apply(due, connection)
                                .context("rammux failed")?,
                            Poll::Ready(Err(timeout)) => return Poll::Ready(Err(timeout.into())),
                            Poll::Pending => break,
                        }
                    }
                    match std::task::ready!(connection.poll_progress(cx)) {
                        Ok(RammuxProgress::Empty) => {},
                        Ok(RammuxProgress::Probe(event)) => self.schedule.observe(event),
                        Ok(RammuxProgress::Inbound(..)) => {
                            return Poll::Ready(Err(anyhow::anyhow!("the server opened a stream")));
                        },
                        Ok(RammuxProgress::Downgraded(downgraded)) => {
                            self.state = RammuxState::Downgrading(downgraded.with_shutdown());
                        },
                        Err(error) => return Poll::Ready(Err(error).context("rammux failed")),
                    }
                },
                RammuxState::Downgrading(downgraded) => {
                    std::task::ready!(downgraded.poll_unpin(cx))
                        .context("rammux downgrade failed")?;
                    self.state = RammuxState::Done;
                },
                RammuxState::Done => return Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        if let RammuxState::Active(..) = self.state {
            let RammuxState::Active(connection) =
                std::mem::replace(&mut self.state, RammuxState::Done)
            else {
                unreachable!("checked above");
            };
            let downgraded = connection.downgrade().context("rammux downgrade failed")?;
            self.state = RammuxState::Downgrading(downgraded.with_shutdown());
        }
        self.poll_drive(cx)
    }
}

// ---------------------------------------------------------------------------
// yamux
// ---------------------------------------------------------------------------

type YamuxTransport = Compat<TokioIo<Upgraded>>;

/// A yamux connection.
pub struct YamuxMuxer {
    connection: yamux::Connection<YamuxTransport>,
}

impl YamuxMuxer {
    pub fn new(upgraded: Upgraded, config: &YamuxMuxerConfig) -> anyhow::Result<Self> {
        Ok(Self {
            connection: yamux::Connection::new(
                TokioIo::new(upgraded).compat(),
                config.to_yamux_config()?,
                yamux::Mode::Client,
            ),
        })
    }
}

impl Muxer for YamuxMuxer {
    type Stream = YamuxStream;

    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Self::Stream>> {
        self.connection.poll_new_outbound(cx).map(|result| {
            result
                .map(|stream| YamuxStream {
                    stream,
                    unwritten: Bytes::new(),
                })
                .context("failed to open a yamux stream")
        })
    }

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        match std::task::ready!(self.connection.poll_next_inbound(cx)) {
            None => Poll::Ready(Ok(())),
            Some(Ok(..)) => Poll::Ready(Err(anyhow::anyhow!("the server opened a stream"))),
            Some(Err(error)) => Poll::Ready(Err(error).context("yamux failed")),
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        self.connection
            .poll_close(cx)
            .map(|result| result.context("yamux close failed"))
    }
}

/// A yamux stream, framed into [`Bytes`].
///
/// yamux streams are byte streams; the codec cuts whatever is readable into
/// one item and writes items back as bytes, which is all the workload needs.
pub struct YamuxStream {
    unwritten: Bytes,
    stream: yamux::Stream,
}

impl Stream for YamuxStream {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .stream
            .poll_next_unpin(cx)
            .map(|item| item.map(|result| result.map(Bytes::from_owner)))
    }
}

impl Sink<Bytes> for YamuxStream {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        loop {
            if this.unwritten.is_empty() {
                break Poll::Ready(Ok(()));
            }
            let result =
                std::task::ready!(Pin::new(&mut this.stream).poll_write(cx, &this.unwritten))?;
            if result == 0 {
                break Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            } else {
                this.unwritten.advance(result);
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.unwritten.is_empty() {
            this.unwritten = item;
            Ok(())
        } else {
            Err(io::Error::other("not ready"))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_ready_unpin(cx))?;
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_ready_unpin(cx))?;
        Pin::new(&mut this.stream).poll_close(cx)
    }
}

// ---------------------------------------------------------------------------
// HTTP/2
// ---------------------------------------------------------------------------

type H2Connection = h2::client::Connection<TokioIo<Upgraded>, Bytes>;

/// An HTTP/2 connection, one request per stream.
///
/// `h2` puts the whole connection in one future: frames move for every open
/// stream while [`Muxer::poll_drive`] polls it, and nothing is spawned, so
/// this runs in one task like the other two protocols.
pub struct H2Muxer {
    /// Dropped to close. With no handle left to open a stream and no stream
    /// still open, the connection sends its GOAWAY and finishes.
    sender: Option<SendRequest<Bytes>>,
    /// The connection, until it finishes.
    connection: Option<H2Connection>,
}

impl H2Muxer {
    pub async fn new(upgraded: Upgraded, config: &H2MuxerConfig) -> anyhow::Result<Self> {
        let (sender, connection) = config
            .to_client_builder()
            .handshake::<_, Bytes>(TokioIo::new(upgraded))
            .await
            .context("HTTP/2 handshake failed")?;
        Ok(Self {
            sender: Some(sender),
            connection: Some(connection),
        })
    }
}

impl Muxer for H2Muxer {
    type Stream = H2Duplex;

    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Self::Stream>> {
        let Some(sender) = self.sender.as_mut() else {
            return Poll::Ready(Err(anyhow::anyhow!("the connection is closed")));
        };
        std::task::ready!(sender.poll_ready(cx)).context("HTTP/2 connection failed")?;
        let request = Request::builder()
            .method(Method::POST)
            .uri("http://echo-server/echo")
            .body(())
            .context("failed to build an HTTP/2 request")?;
        // Not the end of the stream: the body is what the workload sends.
        let (response, send) = sender
            .send_request(request, false)
            .context("failed to open an HTTP/2 stream")?;
        Poll::Ready(Ok(H2Duplex::requested(response, send)))
    }

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        let Some(connection) = self.connection.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        std::task::ready!(connection.poll_unpin(cx)).context("HTTP/2 connection failed")?;
        self.connection = None;
        Poll::Ready(Ok(()))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        self.sender = None;
        self.poll_drive(cx)
    }
}
