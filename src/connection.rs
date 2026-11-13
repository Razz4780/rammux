//! Types for running rammux protocol on an IO transport.

use std::{
    fmt,
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    config::{ConfigError, RammuxConfig, RammuxRole},
    connection::{
        active::{Active, Received},
        state::ConnState,
    },
    error::{ErrorKind, RammuxError},
    stream::RammuxDuplex,
};

pub use crate::connection::downgrade::Downgraded;

mod active;
mod downgrade;
#[cfg(test)]
mod peer_model_test;
mod pings;
#[cfg(test)]
mod sim_test;
mod state;
mod streams;
#[cfg(test)]
mod wire_test;

/// How many inbound frames are processed in a row before outbound frames get a turn.
const INBOUND_BATCH: usize = 16;

/// One side of a rammux connection. It owns the transport, and runs the protocol on it.
///
/// # Polling
///
/// The connection does nothing on its own. Your code has to keep calling [`progress`](Self::progress)
/// or [`poll_progress`](Self::poll_progress). While it does, the connection moves the data of all streams,
/// and reports what happens as [`RammuxProgress`]: a stream that the peer started, or the end of the session.
///
/// If the connection is not polled, its streams stall, and streams that are closed keep their place
/// in the stream limits.
///
/// # Ending the session
///
/// [`downgrade`](Self::downgrade) ends the session and gives the transport back, through the [`Downgraded`]
/// future. The peer may do it first: [`progress`](Self::progress) then returns
/// [`RammuxProgress::Downgraded`], and the connection cannot be used any more.
///
/// # Drop
///
/// Dropping the connection ends all of its streams at once, and the peer is not told.
/// Use [`downgrade`](Self::downgrade) for an orderly end.
pub struct RammuxConnection<IO> {
    state: ConnState<IO>,
    role: RammuxRole,
    config: RammuxConfig,
}

impl<IO> RammuxConnection<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new rammux connection with clean state.
    ///
    /// Fails if the config is not valid, see [`RammuxConfig::validate`].
    ///
    /// # Panics
    ///
    /// Panics if called outside of a Tokio runtime, or in a runtime without the time driver
    /// (`enable_time` or `enable_all` on the runtime builder). The connection uses Tokio timers
    /// for the `PING`s and the inbound frame timeout.
    pub fn new(role: RammuxRole, io: IO, config: RammuxConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            state: ConnState::Active(Active::new(io, role, &config)),
            config,
            role,
        })
    }

    /// Returns the config of this connection.
    pub fn config(&self) -> &RammuxConfig {
        &self.config
    }

    /// Returns the role of this connection.
    pub fn role(&self) -> RammuxRole {
        self.role
    }

    fn poll_progress_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, ErrorKind>> {
        let active = self.state.active_mut()?;

        // Process a bounded batch of inbound frames first: credit can make several streams writable,
        // and the bound keeps the reads from starving the writes.
        let mut progress = Poll::Pending;
        for _ in 0..INBOUND_BATCH {
            match active.poll_receive(cx)? {
                Poll::Pending => break,
                Poll::Ready(Received::Frame(None)) => progress = Poll::Ready(RammuxProgress::Empty),
                Poll::Ready(Received::Frame(Some(stream))) => {
                    progress = Poll::Ready(RammuxProgress::Inbound(stream));
                    break;
                },
                Poll::Ready(Received::Term) => {
                    let downgraded = self.state.downgrade(true)?;
                    return Poll::Ready(Ok(RammuxProgress::Downgraded(downgraded)));
                },
            }
        }

        let _ = active.poll_send(cx)?;
        progress.map(Ok)
    }

    /// Drives the connection, and returns when there is something to report: see [`RammuxProgress`].
    ///
    /// An error is final. The connection cannot be used after it, and all of its streams end.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, RammuxError>> {
        let result = std::task::ready!(self.poll_progress_inner(cx));
        // A failure of the connection is final. A misuse leaves the connection as it is.
        if let Err(error) = &result
            && !error.is_misuse()
        {
            self.state = ConnState::Poisoned;
        }
        Poll::Ready(result.map_err(From::from))
    }

    /// Async sugar for [`Self::poll_progress`].
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel safe. Cancelling it will not disrupt the connection in any way.
    pub async fn progress(&mut self) -> Result<RammuxProgress<IO>, RammuxError> {
        futures::future::poll_fn(|cx| self.poll_progress(cx)).await
    }

    /// Attempts to start a new outbound stream.
    ///
    /// If the configured outbound streams limit is currently exhausted, returns [`None`].
    /// Note that this connection must be polled in order to free IDs of closed streams.
    pub fn try_start_outbound(&mut self) -> Result<Option<RammuxDuplex>, RammuxError> {
        Ok(self.state.active_mut()?.open_outbound())
    }

    /// Starts the downgrade procedure of this connection.
    ///
    /// See [`Downgraded`] doc for more info.
    pub fn downgrade(mut self) -> Result<Downgraded<IO>, RammuxError> {
        self.state.downgrade(false).map_err(From::from)
    }

    /// Returns current statistics of this connection.
    pub fn stats(&self) -> RammuxStats {
        self.state.active().map(Active::stats).unwrap_or_default()
    }
}

impl<IO> fmt::Debug for RammuxConnection<IO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            ConnState::Active(..) => "active",
            ConnState::Poisoned => "poisoned",
            ConnState::Downgraded => "downgraded",
        };
        f.debug_struct("RammuxConnection")
            .field("role", &self.role)
            .field("state", &state)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Progress made by a [`RammuxConnection`].
pub enum RammuxProgress<IO> {
    /// Downgrade procedure was initiated by the other side.
    ///
    /// See [`Downgraded`] doc for more info.
    Downgraded(Downgraded<IO>),
    /// A new inbound stream was started by the other side.
    Inbound(RammuxDuplex),
    /// Some progress was made, but nothing meaningful to report.
    ///
    /// This variant exists only to make [`RammuxConnection::poll_progress`] reliably return control to the caller.
    Empty,
}

impl<IO> fmt::Debug for RammuxProgress<IO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Downgraded(downgraded) => f.debug_tuple("Downgraded").field(downgraded).finish(),
            Self::Inbound(stream) => f.debug_tuple("Inbound").field(stream).finish(),
            Self::Empty => f.write_str("Empty"),
        }
    }
}

/// Statistics of a [`RammuxConnection`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RammuxStats {
    /// How many streams started by the peer are open.
    pub inbound_streams: u32,
    /// How many streams started by this side are open.
    pub outbound_streams: u32,
    /// Smoothed round trip time of the connection, measured with `PING`s.
    ///
    /// It includes the time that data waits in queues on the way, in both directions.
    /// [`None`] until the first measurement.
    pub srtt: Option<Duration>,
    /// Bytes of the peer's receive pool that our streams use.
    ///
    /// At most [`remote_recv_pool`](crate::config::RammuxConfig::remote_recv_pool).
    pub pool_tx_used: u64,
    /// Bytes of our receive pool that the peer's streams use.
    ///
    /// At most [`local_recv_pool`](crate::config::RammuxConfig::local_recv_pool).
    pub pool_rx_used: u64,
    /// How many of our streams wait for their turn to use the peer's pool.
    ///
    /// Streams wait when they have more to send than the pool has room for.
    pub turn_queue_len: u32,
    /// Bytes of the peer's pool that our streams hold as an advance, see
    /// [`advance_grant`](crate::config::RammuxConfig::advance_grant).
    ///
    /// An advance is paid back as the peer's application reads. A value that stays high means that
    /// some streams are not being read. At most
    /// [`advance_budget`](crate::config::RammuxConfig::advance_budget) of the pool.
    pub advance_held: u64,
}

#[cfg(test)]
mod test {
    use std::{
        num::{NonZeroU16, NonZeroU32},
        time::Duration,
    };

    use bytes::Bytes;
    use futures::{FutureExt, SinkExt, StreamExt, stream::FuturesUnordered};
    use rstest::rstest;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
        runtime::Builder,
    };

    use crate::{
        config::{RammuxConfig, RammuxRole},
        connection::{RammuxConnection, RammuxProgress},
        stream::RammuxDuplex,
    };

    const DATA: Bytes = Bytes::from_static(&[b'A'; 64 * 1024]);

    #[test]
    #[should_panic(expected = "no reactor running")]
    fn new_panics_outside_of_a_tokio_runtime() {
        let (io, _peer) = tokio::io::duplex(64);
        let _ = RammuxConnection::new(RammuxRole::Client, io, RammuxConfig::new());
    }

    #[test]
    #[should_panic(expected = "timers are disabled")]
    fn new_panics_without_the_time_driver() {
        let runtime = Builder::new_current_thread().build().unwrap();
        let _guard = runtime.enter();
        let (io, _peer) = tokio::io::duplex(64);
        let _ = RammuxConnection::new(RammuxRole::Client, io, RammuxConfig::new());
    }

    #[rstest]
    #[tokio::test]
    async fn two_sides(#[values(1, 16, 64)] streams: u32) {
        let (io_1, io_2) = tokio::io::duplex(512);
        let mut config = RammuxConfig::new();
        config.frame_limit = NonZeroU16::new(256).unwrap();
        config.local_recv_window = NonZeroU32::new(1024).unwrap();
        config.remote_recv_window = NonZeroU32::new(1024).unwrap();
        config.ping_interval = Duration::from_millis(25);
        config.max_inbound_streams = 16;
        config.max_outbound_streams = 16;
        let conn_1 = RammuxConnection::new(RammuxRole::Client, io_1, config.clone()).unwrap();
        let conn_2 = RammuxConnection::new(RammuxRole::Server, io_2, config.clone()).unwrap();
        tokio::join!(
            run_rammux(conn_1, streams, 32 * 1024).then(verify_io_clean),
            run_rammux(conn_2, streams, 32 * 1024).then(verify_io_clean),
        );
    }

    async fn run_rammux(
        mut conn: RammuxConnection<DuplexStream>,
        streams: u32,
        data_in_stream: usize,
    ) -> DuplexStream {
        let mut remaining_inbound = streams;
        let mut remaining_outbound = streams;
        let mut remaining_finished = streams * 2;
        let mut futs = FuturesUnordered::new();

        let downgraded = loop {
            if remaining_outbound > 0
                && let Some(stream) = conn.try_start_outbound().unwrap()
            {
                remaining_outbound -= 1;
                futs.push(run_stream(stream, data_in_stream));
                continue;
            }
            let progress = tokio::select! {
                Some(..) = futs.next() => {
                    remaining_finished -= 1;
                    if remaining_finished == 0 {
                        break conn.downgrade().unwrap();
                    } else {
                        continue;
                    }
                }
                progress = conn.progress() => progress,
            };
            match progress.unwrap() {
                RammuxProgress::Empty => {},
                RammuxProgress::Downgraded(downgraded) => break downgraded,
                RammuxProgress::Inbound(stream) => {
                    remaining_inbound = remaining_inbound.checked_sub(1).unwrap();
                    futs.push(run_stream(stream, data_in_stream));
                },
            }
        };

        assert_eq!(remaining_outbound, 0);
        assert_eq!(remaining_inbound, 0);
        while futs.next().await.is_some() {}

        downgraded.await.unwrap()
    }

    async fn run_stream(stream: RammuxDuplex, data: usize) {
        let (mut sink, mut stream) = stream.into_split();
        tokio::join!(
            async {
                let mut remaining = data;
                while remaining > 0 {
                    let chunk = if DATA.len() <= remaining {
                        DATA.clone()
                    } else {
                        DATA.clone().split_to(remaining)
                    };
                    remaining -= chunk.len();
                    sink.feed(chunk).await.unwrap();
                }
                sink.close().await.unwrap();
            },
            async {
                let mut read = 0;
                while let Some(chunk) = stream.next().await {
                    read += chunk.len();
                }
                assert_eq!(read, data);
            },
        );
    }

    async fn verify_io_clean(io: DuplexStream) {
        let (mut read, mut write) = tokio::io::split(io);
        tokio::join!(
            async {
                let mut buf = Vec::new();
                read.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, b"B");
            },
            async {
                write.write_all(b"B").await.unwrap();
                write.shutdown().await.unwrap();
            },
        );
    }
}
