//! The benchmarked multiplexers behind one interface, from the client's side.

use std::{
    cell::RefCell,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Context as _;
use async_selector::FutureSelector;
use bytes::{Buf, Bytes};
use futures::{AsyncWrite, FutureExt, Sink, SinkExt, Stream, StreamExt, channel::mpsc};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Body, Incoming},
    client::conn::http2::{Connection, SendRequest},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use quinn::{Endpoint, crypto::rustls::QuicClientConfig};
use rammux::{
    config::RammuxRole,
    connection::{Downgraded, RammuxConnection, RammuxProgress},
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::{
    config::{H2MuxerConfig, QuicMuxerConfig, RammuxMuxerConfig, YamuxMuxerConfig},
    rammux_rtt::RttSchedule,
    stream_util::{ChannelBody, QuicDuplex, RammuxIo},
    tls,
};

/// A multiplexed connection to the echo server.
///
/// The four protocols differ in how a connection is driven, how a stream is
/// opened and how the connection is closed - and QUIC differs in how it is
/// reached at all; the workload does not care about any of that.
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

/// An HTTP/2 connection, one request per stream.
///
/// hyper's client splits the work between a dispatcher, which is the
/// [`Connection`] future it hands back, and futures it spawns on an executor:
/// the connection driver that actually moves frames, and a body pipe per
/// request. [`InlineExecutor`] captures those, and [`Muxer::poll_drive`] runs
/// them here - so the whole client runs in one task, like the other two
/// protocols, and closing can wait for the wire to actually close.
pub struct H2Muxer {
    /// Dropped to close: hyper shuts the connection down once the last
    /// request handle is gone.
    sender: Option<SendRequest<ChannelBody>>,
    /// The dispatcher, until it finishes.
    dispatcher: Option<Connection<Upgraded, ChannelBody, InlineExecutor>>,
    /// Everything hyper spawned.
    executor: InlineExecutor,
}

/// A future hyper asked the executor to run.
type SpawnedTask = Pin<Box<dyn Future<Output = ()>>>;

/// A [`hyper::rt::Executor`] that does not spawn: it stores futures in a [`FutureSelector`],
/// for [`H2Muxer::poll_drive`] to poll.
#[derive(Clone, Default)]
pub struct InlineExecutor(Rc<RefCell<FutureSelector<SpawnedTask>>>);

impl InlineExecutor {
    /// Polls captured futures.
    ///
    /// Returns whether any are still running.
    fn poll_tasks(&self, cx: &mut Context<'_>) -> bool {
        let mut tasks = self.0.borrow_mut();
        loop {
            match tasks.poll_next_unpin(cx) {
                Poll::Pending => break true,
                Poll::Ready(Some(())) => {},
                Poll::Ready(None) => break false,
            }
        }
    }
}

impl<F> hyper::rt::Executor<F> for InlineExecutor
where
    F: Future<Output = ()> + 'static,
{
    fn execute(&self, future: F) {
        self.0.borrow_mut().push(Box::pin(future));
    }
}

impl H2Muxer {
    pub async fn new(upgraded: Upgraded, config: &H2MuxerConfig) -> anyhow::Result<Self> {
        let executor = InlineExecutor::default();
        let (sender, dispatcher) = hyper::client::conn::http2::Builder::new(executor.clone())
            .keep_alive_interval(None)
            .max_frame_size(16 * 1024)
            .max_concurrent_streams(100)
            .initial_connection_window_size(config.global_recv_window)
            .initial_stream_window_size(config.stream_recv_window)
            .adaptive_window(config.adaptive_window)
            .handshake(upgraded)
            .await
            .context("HTTP/2 handshake failed")?;
        Ok(Self {
            sender: Some(sender),
            dispatcher: Some(dispatcher),
            executor,
        })
    }
}

impl Muxer for H2Muxer {
    type Stream = H2Stream;

    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Self::Stream>> {
        let Some(sender) = self.sender.as_mut() else {
            return Poll::Ready(Err(anyhow::anyhow!("the connection is closed")));
        };
        std::task::ready!(sender.poll_ready(cx)).context("HTTP/2 connection failed")?;
        let (outgoing, body) = mpsc::channel(1);
        let request = Request::builder()
            .method(Method::POST)
            .uri("http://echo-server/echo")
            .body(ChannelBody(body))
            .context("failed to build an HTTP/2 request")?;
        Poll::Ready(Ok(H2Stream {
            response: Some(Box::pin(sender.send_request(request))),
            incoming: None,
            outgoing,
        }))
    }

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        // The dispatcher queues requests for the driver; the driver and the
        // body pipes are in the executor. The connection is over when all
        // of them are.
        if let Some(dispatcher) = self.dispatcher.as_mut()
            && let Poll::Ready(result) = dispatcher.poll_unpin(cx)
        {
            result.context("HTTP/2 connection failed")?;
            self.dispatcher = None;
        }
        let tasks_running = self.executor.poll_tasks(cx);
        if self.dispatcher.is_none() && !tasks_running {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        self.sender = None;
        self.poll_drive(cx)
    }
}

/// The response to a request, on its way.
type ResponseFuture = Pin<Box<dyn Future<Output = hyper::Result<Response<Incoming>>>>>;

/// An HTTP/2 stream: the request body goes out, the response body comes back.
pub struct H2Stream {
    /// The response, until it arrives.
    response: Option<ResponseFuture>,
    /// The response body, once it arrives.
    incoming: Option<Incoming>,
    /// Feeds the request body.
    outgoing: mpsc::Sender<Bytes>,
}

impl Stream for H2Stream {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(response) = this.response.as_mut() {
            let response = std::task::ready!(response.poll_unpin(cx)).map_err(io::Error::other)?;
            this.response = None;
            if response.status() != StatusCode::OK {
                return Poll::Ready(Some(Err(io::Error::other(format!(
                    "server responded with {}",
                    response.status()
                )))));
            }
            this.incoming = Some(response.into_body());
        }
        let incoming = this.incoming.as_mut().expect("set above");
        loop {
            match std::task::ready!(Pin::new(&mut *incoming).poll_frame(cx)) {
                None => return Poll::Ready(None),
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        return Poll::Ready(Some(Ok(data)));
                    }
                },
                Some(Err(error)) => return Poll::Ready(Some(Err(io::Error::other(error)))),
            }
        }
    }
}

impl Sink<Bytes> for H2Stream {
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

// ---------------------------------------------------------------------------
// QUIC
// ---------------------------------------------------------------------------

/// A QUIC connection, one bidirectional stream per logical stream.
///
/// The odd one out in this file. QUIC carries its own TLS and its own
/// congestion control, so there is no upgrade to ride and no plaintext
/// variant, and `quinn` services the socket from the endpoint's own task
/// rather than from a future the caller polls. [`Muxer::poll_drive`] therefore
/// has nothing to move; it only watches for the connection ending, which is
/// what driving it reports for every other protocol here.
pub struct QuicMuxer {
    connection: quinn::Connection,
    /// Held only to keep it alive: dropping the endpoint stops the task that
    /// services the socket, and the connection would go with it.
    _endpoint: Endpoint,
    /// Resolves when the connection ends, however it ends. Taken once it
    /// has: [`Muxer::poll_close`] drives this to completion and then keeps
    /// polling for the endpoint to go idle, and a resolved future must not be
    /// polled again.
    closed: Option<Pin<Box<dyn Future<Output = quinn::ConnectionError>>>>,
    /// Set once the close has been asked for, so it is asked for once.
    closing: bool,
}

impl QuicMuxer {
    /// Connects, and hands the server the config it should be running.
    ///
    /// The server cannot take its windows from what the client sends: they are
    /// transport parameters, so the handshake has already fixed them by the
    /// time any stream data arrives. It runs from its own config instead, and
    /// this only tells it what to compare against - a mismatch closes the
    /// connection rather than quietly measuring two different settings.
    pub async fn connect(
        server_addr: SocketAddr,
        cert_path: Option<&Path>,
        config: &QuicMuxerConfig,
    ) -> anyhow::Result<Self> {
        let cert_path = cert_path.context(
            "a QUIC run needs `cert_path`: QUIC is always encrypted, so there is no plaintext \
             variant to run without one",
        )?;
        let crypto = tls::CertBundle::read(cert_path)?.client_config()?;
        let crypto =
            QuicClientConfig::try_from(crypto).context("the TLS config cannot be used for QUIC")?;
        let mut client_config = quinn::ClientConfig::new(Arc::new(crypto));
        client_config.transport_config(Arc::new(config.to_transport_config()));

        // Whatever local address matches the server's family; the benchmark
        // never needs a fixed one.
        let bind: SocketAddr = if server_addr.is_ipv4() {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            (Ipv6Addr::UNSPECIFIED, 0).into()
        };
        let mut endpoint = Endpoint::client(bind).context("failed to bind a QUIC socket")?;
        endpoint.set_default_client_config(client_config);

        let name = tls::server_name().to_str().into_owned();
        let connection = endpoint
            .connect(server_addr, &name)
            .context("failed to start a QUIC connection")?
            .await
            .with_context(|| format!("QUIC handshake with {server_addr} failed"))?;

        let mut control = connection
            .open_uni()
            .await
            .context("failed to open the QUIC control stream")?;
        let encoded = serde_json::to_vec(config).expect("the config serializes");
        control
            .write_all(&encoded)
            .await
            .context("failed to send the QUIC config")?;
        control
            .finish()
            .context("failed to finish the QUIC control stream")?;

        let closed = {
            let connection = connection.clone();
            Some(Box::pin(async move { connection.closed().await }) as Pin<Box<_>>)
        };
        Ok(Self {
            connection,
            _endpoint: endpoint,
            closed,
            closing: false,
        })
    }
}

impl Muxer for QuicMuxer {
    type Stream = QuicDuplex;

    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Self::Stream>> {
        // `open_bi` borrows the connection, so its future cannot be parked in
        // a field here. Both the clone and the future are cheap, and a stream
        // is opened once, so it is rebuilt per poll instead.
        let mut opening = Box::pin(self.connection.open_bi());
        opening.as_mut().poll(cx).map(|result| {
            result
                .map(|(send, recv)| QuicDuplex::new(send, recv))
                .context("failed to open a QUIC stream")
        })
    }

    fn poll_drive(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        let Some(closed) = self.closed.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let error = std::task::ready!(closed.as_mut().poll(cx));
        self.closed = None;
        match error {
            // Our own close, or the peer's clean one.
            quinn::ConnectionError::LocallyClosed
            | quinn::ConnectionError::ApplicationClosed(..) => Poll::Ready(Ok(())),
            error => Poll::Ready(Err(
                anyhow::Error::new(error).context("QUIC connection failed")
            )),
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        if !self.closing {
            self.closing = true;
            self.connection.close(0u32.into(), b"done");
        }
        // `close` puts the CONNECTION_CLOSE on the wire itself, so waiting for
        // the connection to reach its closed state is enough for the peer to
        // see a clean shutdown rather than a timeout.
        //
        // Not `Endpoint::wait_idle`, which is the obvious-looking thing to
        // reach for: it waits until the endpoint has no connections left, and
        // this one is still held right here, so it would never return.
        self.poll_drive(cx)
    }
}
