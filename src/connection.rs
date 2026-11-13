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

/// State machine of a single rammux connection.
///
/// # Polling
///
/// This state machine does not run anything in the background on its own.
/// Your code must keep polling [`RammuxConnection::progress`] or [`RammuxConnection::poll_progress`].
/// While the connection is being polled, it will:
///
/// 1. read and decode inbound frames,
/// 2. encode and flush outbound frames from active streams,
/// 3. surface newly accepted inbound streams as
///    [`RammuxProgress::Inbound`], and
/// 4. surface remotely initiated downgrade handshake as
///    [`RammuxProgress::Downgraded`].
///
/// If the connection stops being polled, stream IO stalls, flow-control updates
/// stop, and closed stream IDs are not reclaimed.
///
/// # Downgrade
///
/// To stop using rammux and recover the wrapped transport, call
/// [`RammuxConnection::downgrade`] and await the returned [`Downgraded`].
/// That future sends the final `TERM` frame, waits for the peer's `TERM`,
/// and yields a clean transport with no unread rammux bytes left in it.
///
/// Note that the other side might start the downgrade first.
/// In this case, [`RammuxConnection::progress`]/[`RammuxConnection::poll_progress`]
/// will yield [`RammuxProgress::Downgraded`], and [`RammuxConnection`] will no longer be usable.
///
/// # Drop
///
/// Dropping this struct while the rammux connection is open will abruptly close the connection and all rammux streams.
/// Proper rammux shutdown requires that [`Downgraded`] is polled to completion.
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

    /// Makes progress in this connection.
    ///
    /// See [`RammuxProgress`] doc for more info.
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
    /// Count of currently active inbound streams.
    pub inbound_streams: u32,
    /// Count of currently active outbound streams.
    pub outbound_streams: u32,
    /// Smoothed round trip time, measured with `PING`/`PONG` exchanges.
    ///
    /// It includes the queues on the path in both directions, and the delay in the peer's muxer.
    /// Empty if no `PONG` has been received yet.
    pub srtt: Option<Duration>,
    /// Bytes of the peer's receive pool used by our outbound data.
    ///
    /// The sum over the streams of the bytes sent and not credited back, beyond the base window of each stream.
    /// It never exceeds the configured [`remote_recv_pool`](crate::config::RammuxConfig::remote_recv_pool).
    pub pool_tx_used: u64,
    /// Bytes of our receive pool used by the inbound data of the peer.
    ///
    /// The sum over the streams of the bytes received and not credited back, beyond the base window of each stream.
    /// It never exceeds the configured [`local_recv_pool`](crate::config::RammuxConfig::local_recv_pool):
    /// a peer that would make it do so violates the protocol, and the connection fails.
    pub pool_rx_used: u64,
    /// Count of streams waiting for their turn to use the shared pool.
    pub turn_queue_len: u32,
    /// Bytes of the pool that our outbound streams hold as an advance: sent before the peer's reader proved to read.
    ///
    /// Zero unless the advance is on, and at most [`advance_budget`](crate::config::RammuxConfig::advance_budget)
    /// of the pool. A value that stays high means that some readers of the peer stalled.
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
