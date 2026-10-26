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
    config::{ConfigError, RammuxConfig, RammuxRole, TransitControl},
    connection::{
        pings::PingSchedule,
        state::{Active, ConnState},
    },
    delay::{DelayControl, PongSample},
    error::{ErrorKind, RammuxError},
    global_pool::GlobalPool,
    header::Pong,
    stream::RammuxDuplex,
    stream_id::StreamId,
};

pub use crate::connection::downgrade::Downgraded;

mod downgrade;
mod pings;
#[cfg(test)]
mod sim_test;
mod state;
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
    pub fn new(role: RammuxRole, io: IO, config: RammuxConfig) -> Result<Self, ConfigError> {
        Self::with_clock(role, io, config, Clock::new())
    }

    /// Like [`Self::new`], with the given private clock.
    pub(crate) fn with_clock(
        role: RammuxRole,
        io: IO,
        config: RammuxConfig,
        clock: Clock,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let mut global = GlobalPool::new(config.local_recv_pool, config.remote_recv_pool);
        if config.transit_control == TransitControl::DelayControl {
            global = global.with_delay_control(DelayControl::new(
                config.queue_target_bytes.get(),
                config.frame_limit.get(),
            ));
        }
        Ok(Self {
            state: ConnState::Active(Active {
                codec: RammuxCodec::new(
                    io,
                    config.frame_limit,
                    config.inbound_frame_timeout,
                    clock,
                ),
                streams: Default::default(),
                selector: Selector::new(global),
                pings: PingSchedule::new(config.ping_interval, config.ping_bytes.get()),
                pong: None,
                last_echo: None,
                pacer: Box::pin(tokio::time::sleep(Duration::ZERO)),
                pacer_armed: false,
                pacer_deadline: tokio::time::Instant::now(),
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
            // A PONG answers one of our PINGs, and the peer answers only the latest one it has read,
            // so the echoes of a sound peer strictly increase. Whatever is not later than the last
            // accepted echo (a duplicate, a reordered PONG), or later than the newest stamp we have
            // sent (an echo of a PING that never existed), is ignored. The age of the echo does not
            // matter: a PONG can wait in a queue for as long as the queue is.
            let sent = active.codec.newest_ping_stamp();
            let fresh = active
                .last_echo
                .is_none_or(|last| clock::diff(pong.t_echo, last) > 0);
            if fresh
                && sent.is_some_and(|sent| clock::diff(pong.t_echo, sent) <= 0)
                && let Some(rtt) = clock::elapsed(now, pong.t_echo)
            {
                active.last_echo = Some(pong.t_echo);
                global.on_pong(
                    PongSample {
                        now: header_at,
                        now_us: now,
                        t_echo: pong.t_echo,
                        t_read: pong.t_read,
                        rx_bytes: pong.rx_bytes,
                    },
                    rtt,
                );
                if global.dc.is_some() {
                    // The window may have grown, and the delivered bytes may have left it.
                    global.wake_front(None);
                }
            }
        }
        if let Some(t_ping) = header.ping {
            // Only the latest PING is answered, see `Active::pong`.
            // The counter does not include the payload of this frame.
            active.pong = Some(Pong {
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
                .received(
                    stream.credit,
                    data,
                    flags.fin_write,
                    flags.fin_read,
                    active.selector.strategy_mut(),
                )
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
                .received(
                    stream.credit,
                    data,
                    flags.fin_write,
                    flags.fin_read,
                    active.selector.strategy_mut(),
                )
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

    /// Decides whether a `PING` goes out now, and if so, accounts for it.
    ///
    /// With `DelayControl`, the window needs a sample every round trip: a `PING` is due after
    /// `W / 4` bytes, and when the window keeps a frame from being sent.
    fn poll_ping(active: &mut Active<IO>, cx: &mut Context<'_>) -> bool {
        let global = active.selector.strategy_mut();
        let tx_bytes = global.tx_bytes;
        let (cap, blocked) = global.dc.as_ref().map_or((u32::MAX, false), |dc| {
            let cap = u32::try_from(dc.window() / 4).unwrap_or(u32::MAX);
            (cap, dc.peek_ping_wanted())
        });
        if active
            .pings
            .poll_due(cx, tx_bytes, cap, blocked)
            .is_pending()
        {
            return false;
        }
        active.pings.sent(tx_bytes);
        if let Some(dc) = &mut global.dc {
            dc.take_ping_wanted();
        }
        true
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
            let mut frame = OutFrame {
                pong: active.pong.take(),
                ping: Self::poll_ping(active, cx),
                ..Default::default()
            };

            let mut finished = None;
            if let Poll::Ready(Some((update, fin_state))) = active.selector.poll_next_unpin(cx) {
                finished = fin_state.is_dead().then_some(update.id);
                frame.stream = Some(update.into());
            }

            if !frame.ping {
                // The streams may have found the window in the way, which makes a PING due.
                frame.ping = Self::poll_ping(active, cx);
            }
            if frame.is_empty() {
                // The pacer kept a frame back: wake the streams when it has room for it.
                let global = active.selector.strategy_mut();
                if let Some(deadline) = global
                    .dc
                    .as_mut()
                    .and_then(DelayControl::take_pace_deadline)
                {
                    active.pacer.as_mut().reset(deadline);
                    active.pacer_deadline = deadline;
                    active.pacer_armed = true;
                }
                if active.pacer_armed && active.pacer.as_mut().poll(cx).is_ready() {
                    active.pacer_armed = false;
                    let global = active.selector.strategy_mut();
                    if let Some(dc) = &mut global.dc {
                        dc.note_timer(active.pacer_deadline, tokio::time::Instant::now());
                    }
                    global.wake_front(None);
                    continue;
                }
                let _ = active.codec.poll_flush_unpin(cx)?;
                break Poll::Pending;
            }

            let global = active.selector.strategy_mut();
            if let Some(dc) = &mut global.dc
                && frame.data_len() > 0
            {
                dc.on_sent(tokio::time::Instant::now(), frame.data_len() as u64);
            }
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
        let _ = self.make_outbound_progress(cx)?;
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
        let Ok(active) = self.state.active() else {
            return RammuxStats::default();
        };
        let global = active.selector.strategy();
        let delay = global
            .dc
            .as_ref()
            .map(|dc| dc.stats(tokio::time::Instant::now(), global.tx_bytes))
            .unwrap_or_default();
        RammuxStats {
            inbound_streams: u32::try_from(active.streams.inbound.len())
                .expect("we can't have more than u32 inbound streams"),
            outbound_streams: u32::try_from(active.streams.outbound.len())
                .expect("we can't have more than u32 outbound streams"),
            srtt: global.srtt,
            pool_tx_used: global.tx_used,
            pool_rx_used: global.rx_used,
            turn_queue_len: u32::try_from(global.turns_len()).expect(
                "at most one queue entry per stream, and there are fewer than u32::MAX streams",
            ),
            q_fwd: delay.q_fwd,
            q_back: delay.q_back,
            own: delay.own,
            rate: delay.rate,
            fwd_in_flight: delay.fwd_in_flight,
            window: delay.window,
            rebaselines_fwd: delay.rebaselines_fwd,
            rebaselines_back: delay.rebaselines_back,
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
    /// The queue on our forward path, from the one-way delay of `PING`s. Only with `DelayControl`.
    pub q_fwd: Duration,
    /// How long the reports of the peer currently wait on their way back to us. `DelayControl`.
    pub q_back: Duration,
    /// Bytes of ours in the queue of the bottleneck, `rate * q_fwd`. `DelayControl`.
    pub own: u64,
    /// The delivery rate, in bytes per second, from the bytes the peer reports. `DelayControl`.
    pub rate: f64,
    /// Bytes on the forward path, as the connection window counts them. `DelayControl`.
    pub fwd_in_flight: u64,
    /// The connection window, in bytes. `DelayControl`.
    pub window: u64,
    /// How many times the base delay of the forward path was raised. `DelayControl`.
    pub rebaselines_fwd: u64,
    /// How many times the base delay of the way back was raised. `DelayControl`.
    pub rebaselines_back: u64,
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
