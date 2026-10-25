//! Types for running rammux protocol on an IO transport.

use std::{
    collections::hash_map::Entry,
    convert::Infallible,
    io,
    num::NonZeroU16,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::selector::Selector;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    clock::{self, Clock},
    codec::{
        RammuxCodec,
        decoder::{DecodedFrame, InboundFrame},
        encoder::OutFrame,
    },
    config::{ConfigError, RammuxConfig, RammuxRole},
    connection::{
        pings::PingSchedule,
        state::{Active, ConnState},
    },
    error::{ErrorKind, RammuxError},
    global_pool::GlobalPool,
    header::Pong,
    stream::RammuxDuplex,
    stream_id::StreamId,
};

pub use crate::connection::downgrade::Downgraded;

mod downgrade;
mod pings;
mod state;
#[cfg(test)]
mod wire_test;

/// How many inbound frames are processed in a row before outbound frames get a turn.
const INBOUND_BATCH: usize = 16;

/// How many `PONG`s a connection may owe before it stops reading.
///
/// Every `PING` is answered, and a frame carries at most one `PONG`. A peer that sends `PING`s faster
/// than we can write the answers (or that does not read them at all) would make the queue grow
/// without bound. Past this many, we stop reading until the queue shrinks: the memory is bounded,
/// and the peer is pushed back through the transport (TCP's window), like by any other slow reader.
const MAX_OWED_PONGS: usize = 256;

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
    pub fn new(role: RammuxRole, io: IO, config: RammuxConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let clock = Clock::new();
        Ok(Self {
            state: ConnState::Active(Active {
                codec: RammuxCodec::new(
                    io,
                    config.frame_limit,
                    config.inbound_frame_timeout,
                    clock,
                ),
                streams: Default::default(),
                selector: Selector::new(GlobalPool {
                    available: config.global_recv_window,
                    ..Default::default()
                }),
                pings: PingSchedule::new(config.ping_interval, config.ping_bytes.get()),
                pongs: Default::default(),
                clock,
            }),
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

    fn poll_inbound_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, ErrorKind>> {
        let active = self.state.active_mut()?;
        if active.pongs.len() >= MAX_OWED_PONGS {
            // The caller wakes us up when the owed PONGs drop below the cap.
            return Poll::Pending;
        }
        let frame = std::task::ready!(active.codec.poll_next_unpin(cx))
            .ok_or(io::ErrorKind::UnexpectedEof)
            .map_err(io::Error::from)??;

        let frame = match frame {
            DecodedFrame::Frame(frame) => frame,
            DecodedFrame::Terminate => {
                let downgraded = self.state.downgrade(true)?;
                return Poll::Ready(Ok(RammuxProgress::Downgraded(downgraded)));
            },
        };
        let InboundFrame {
            header,
            data,
            header_at,
        } = frame;

        // The connection part of the frame comes first.
        let global = active.selector.strategy_mut();
        let now = active.clock.micros(header_at);
        if let Some(pong) = header.pong {
            // A PONG is welcome even if we have no idea what it answers.
            // If it seems to come from the future, it does not make sense, and we ignore it.
            if let Some(rtt) = clock::elapsed(now, pong.t_echo) {
                global.rtt = Some(rtt);
            }
        }
        if let Some(t_ping) = header.ping {
            // The counter does not include the payload of this frame.
            active.pongs.push_back(Pong {
                t_echo: t_ping,
                t_read: now,
                rx_bytes: global.rx_bytes,
            });
        }

        let Some(stream) = header.stream else {
            return Poll::Ready(Ok(RammuxProgress::Empty));
        };
        let stream_id = stream.id;
        let flags = stream.flags;
        global.rx_bytes = global
            .rx_bytes
            .wrapping_add(u32::from(stream.data_len.map_or(0, NonZeroU16::get)));

        let progress = if stream_id.initiated_by() == self.role {
            if flags.syn {
                return Poll::Ready(Err(ErrorKind::Stream {
                    id: stream_id,
                    error: "started a new stream with an ID from the wrong pool".into(),
                }));
            }
            let slab_idx = stream_id.slab_idx();
            let e = active
                .streams
                .outbound
                .get_mut(slab_idx)
                .ok_or(ErrorKind::Stream {
                    id: stream_id,
                    error: "sent a frame for an unknown stream".into(),
                })?;

            let fin_state = e
                .received(stream.credit, data, flags.fin_write, flags.fin_read)
                .map_err(|error| ErrorKind::Stream {
                    id: stream_id,
                    error,
                })?;
            if fin_state.is_dead() {
                active.streams.outbound.remove(slab_idx);
            }

            RammuxProgress::Empty
        } else {
            let stream_count = active.streams.inbound.len();
            let (mut e, new_stream) = match active.streams.inbound.entry(stream_id) {
                Entry::Occupied(..) if flags.syn => {
                    return Poll::Ready(Err(ErrorKind::Stream {
                        id: stream_id,
                        error: "started a new stream with an occupied ID".into(),
                    }));
                },
                Entry::Occupied(e) => (e, None),
                Entry::Vacant(e) if flags.syn => {
                    if stream_count == crate::safe_cast_usize(self.config.max_inbound_streams) {
                        return Poll::Ready(Err(ErrorKind::Stream {
                            id: stream_id,
                            error: "started a new stream without respecting the configured limit"
                                .into(),
                        }));
                    }
                    let (handle, updates, duplex) =
                        crate::stream::new(stream_id, false, &self.config);
                    active.selector.push(updates);
                    (e.insert_entry(handle), Some(duplex))
                },
                Entry::Vacant(..) => {
                    return Poll::Ready(Err(ErrorKind::Stream {
                        id: stream_id,
                        error: "sent a frame for an unknown stream".into(),
                    }));
                },
            };

            let fin_state = e
                .get_mut()
                .received(stream.credit, data, flags.fin_write, flags.fin_read)
                .map_err(|error| ErrorKind::Stream {
                    id: stream_id,
                    error,
                })?;
            if fin_state.is_dead() {
                e.remove();
            }

            new_stream
                .map(RammuxProgress::Inbound)
                .unwrap_or(RammuxProgress::Empty)
        };

        Poll::Ready(Ok(progress))
    }

    fn make_outbound_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Infallible, ErrorKind>> {
        let active = self.state.active_mut()?;

        loop {
            std::task::ready!(active.codec.poll_ready_unpin(cx))?;

            // Compose the next frame. The order of importance is:
            // 1. PONG, the peer measures its round trip with it,
            // 2. PING, same,
            // 3. stream control,
            // 4. stream data.
            // What goes first cannot wait behind what goes later,
            // but it rides along if the later frame is going out anyway.
            let mut frame = OutFrame::default();
            let tx_bytes = active.selector.strategy().tx_bytes;

            frame.pong = active.pongs.pop_front();
            if active.pings.poll_due(cx, tx_bytes).is_ready() {
                active.pings.sent(tx_bytes);
                frame.ping = true;
            }

            let mut finished = None;
            if let Poll::Ready(Some((update, fin_state))) = active.selector.poll_next_unpin(cx) {
                finished = fin_state.is_dead().then_some(update.id);
                frame.stream = Some(update.into());
            }

            if frame.is_empty() {
                let _ = active.codec.poll_flush_unpin(cx)?;
                break Poll::Pending;
            }

            let global = active.selector.strategy_mut();
            global.tx_bytes = global.tx_bytes.wrapping_add(
                u32::try_from(frame.data_len()).expect("a frame carries at most u16::MAX bytes"),
            );
            active.codec.start_send_unpin(frame)?;

            if let Some(id) = finished {
                if id.initiated_by() == self.role {
                    active.streams.outbound.remove(id.slab_idx());
                } else {
                    active.streams.inbound.remove(&id);
                }
            }
        }
    }

    fn owes_too_many_pongs(&self) -> bool {
        self.state
            .active()
            .is_ok_and(|active| active.pongs.len() >= MAX_OWED_PONGS)
    }

    fn poll_progress_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, ErrorKind>> {
        let mut inbound = Poll::Pending;
        // Drain a bounded batch of inbound frames first.
        // Processing window updates can make several outbound streams writable.
        // Keep the work bounded so ready reads cannot starve writes.
        for _ in 0..INBOUND_BATCH {
            match self.poll_inbound_progress(cx)? {
                Poll::Pending => break,
                Poll::Ready(RammuxProgress::Empty) => {
                    inbound = Poll::Ready(RammuxProgress::Empty);
                },
                Poll::Ready(other) => {
                    inbound = Poll::Ready(other);
                    break;
                },
            }
        }
        if let Poll::Ready(RammuxProgress::Downgraded(..)) = inbound {
            return inbound.map(Ok);
        }
        let was_capped = self.owes_too_many_pongs();
        let _ = self.make_outbound_progress(cx)?;
        if was_capped && !self.owes_too_many_pongs() {
            // Reading was paused, and the codec has no reason to wake us.
            cx.waker().wake_by_ref();
        }
        inbound.map(Ok)
    }

    /// Makes progress in this connection.
    ///
    /// See [`RammuxProgress`] doc for more info.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, RammuxError>> {
        let result = std::task::ready!(self.poll_progress_inner(cx));
        match &result {
            Ok(RammuxProgress::Empty | RammuxProgress::Inbound(..)) => {},
            Ok(RammuxProgress::Downgraded(..)) => {},
            Err(ErrorKind::AlreadyDowngraded | ErrorKind::Poisoned) => {},
            Err(..) => {
                self.state = ConnState::Poisoned;
            },
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
        let active = self.state.active_mut()?;

        if active.streams.outbound.len() >= crate::safe_cast_usize(self.config.max_outbound_streams)
        {
            return Ok(None);
        }
        let slab_idx = active.streams.outbound.vacant_key();
        let Some(id) = StreamId::from_slab_idx(slab_idx, self.role) else {
            return Ok(None);
        };

        let (handle, updates, duplex) = crate::stream::new(id, true, &self.config);
        active.selector.push(updates);
        active.streams.outbound.insert(handle);

        Ok(Some(duplex))
    }

    /// Starts the downgrade procedure of this connection.
    ///
    /// See [`Downgraded`] doc for more info.
    pub fn downgrade(mut self) -> Result<Downgraded<IO>, RammuxError> {
        self.state.downgrade(false).map_err(From::from)
    }

    /// Returns current statistics of this connection.
    pub fn stats(&self) -> RammuxStats {
        let (inbound_streams, outbound_streams, rtt, available_global_recv_window) = self
            .state
            .active()
            .map(|active| {
                let global = active.selector.strategy();
                (
                    u32::try_from(active.streams.inbound.len())
                        .expect("we can't have more than u32 inbound streams"),
                    u32::try_from(active.streams.outbound.len())
                        .expect("we can't have more than u32 outbound streams"),
                    global.rtt,
                    global.available,
                )
            })
            .unwrap_or_default();

        RammuxStats {
            inbound_streams,
            outbound_streams,
            rtt,
            available_global_recv_window,
        }
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

/// Statistics of a [`RammuxConnection`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RammuxStats {
    /// Count of currently active inbound streams.
    pub inbound_streams: u32,
    /// Count of currently active outbound streams.
    pub outbound_streams: u32,
    /// Most recent round trip time, measured with a `PING` exchange.
    ///
    /// Empty if no `PING` response has been received yet.
    pub rtt: Option<Duration>,
    /// Bytes available in the global receive window pool.
    pub available_global_recv_window: usize,
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use crate::{
        config::{RammuxConfig, RammuxRole},
        connection::{RammuxConnection, RammuxProgress},
        stream::RammuxDuplex,
    };

    const DATA: Bytes = Bytes::from_static(&[b'A'; 64 * 1024]);

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
