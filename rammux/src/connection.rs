//! Types for running rammux protocol on an IO transport.

use std::{
    collections::hash_map::Entry,
    convert::Infallible,
    io,
    num::NonZeroU32,
    ops::Not,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::selector::Selector;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    codec::{
        self, RammuxCodec,
        decoder::{DecodedFrame, StreamPayload},
        encoder::EncoderItem,
    },
    config::{RammuxConfig, RammuxRole},
    connection::state::{Active, ConnState},
    error::{ErrorKind, RammuxError},
    global_pool::{GlobalPool, TransitRecv, TransitSend},
    probe::{Probe, ProbeDone, ProbeFrame},
    stream::RammuxDuplex,
    stream_id::StreamId,
};

pub use crate::{connection::downgrade::Downgraded, probe::ProbeEvent};

mod downgrade;
mod state;

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
///    [`RammuxProgress::Inbound`],
/// 4. surface remotely initiated downgrade handshake as
///    [`RammuxProgress::Downgraded`], and
/// 5. surface RTT-measurement transitions as [`RammuxProgress::Probe`].
///
/// If the connection stops being polled, stream IO stalls, flow-control updates
/// stop, and closed stream IDs are not reclaimed.
///
/// # RTT measurement
///
/// A connection runs no timers of its own. Nothing here sleeps, and the
/// only thing that ever wakes the connection's task is the transport.
/// The two `PING` mechanisms are therefore started by your code:
///
/// - [`RammuxConnection::send_ping`] measures the *loaded* RTT with a
///   `PING` that travels inline with data. Stream receive windows are
///   sized from it.
/// - [`RammuxConnection::start_probe`] measures the *clean* RTT: it
///   pauses data output on both sides, drains the link, and times a
///   `PING` over it. The session-level transit window is sized from it,
///   so a connection that never probes never autotunes.
///
/// Neither is given up on. Every transition is reported as a
/// [`RammuxProgress::Probe`], which is what lets your code impose the
/// schedule and the deadlines it wants - including the deadline on
/// [`ProbeEvent::ProbeStarted`] that detects a dead peer, since an
/// unfinished probe pauses data output indefinitely.
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
    pub fn new(role: RammuxRole, io: IO, config: RammuxConfig) -> Self {
        Self {
            state: ConnState::Active(Active {
                codec: RammuxCodec::new(io, config.frame_limit),
                streams: Default::default(),
                selector: Selector::new(GlobalPool {
                    rtt: None,
                    available: config.global_recv_window,
                    probe: Probe::new(role),
                    transit_send: (config.remote_transit_window > 0).then_some(TransitSend {
                        credit: config.remote_transit_window,
                    }),
                    transit_recv: NonZeroU32::new(config.local_transit_window).map(|initial| {
                        TransitRecv::new(
                            initial,
                            config.transit_window_max,
                            config.transit_update_threshold,
                        )
                    }),
                    dirty_rtt: None,
                    growth: config.transit_growth,
                    transit_blocked: false,
                    stalled_since: None,
                    stalled_total: Duration::ZERO,
                    stalled_events: 0,
                }),
            }),
            config,
            role,
        }
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

        match &frame {
            // Past the peer's CLEAR_LINK its direction is drained: only
            // probe frames (and a downgrade) may arrive until the probe
            // runs its course on the peer's side.
            DecodedFrame::Stream { .. } | DecodedFrame::SessionWindowUpdate { .. }
                if active.selector.strategy().probe.peer_must_be_silent() =>
            {
                return Poll::Ready(Err(ErrorKind::Probe(
                    "received a non-probe frame while the link is being cleared",
                )));
            },
            DecodedFrame::Stream {
                payload: StreamPayload::Data(data),
                ..
            } => {
                // Transit credit is freed as soon as the data is stored in this muxer.
                active
                    .selector
                    .strategy_mut()
                    .transit_recv_freed(data.as_ref().len());
            },
            _ => {},
        }

        let progress = match frame {
            DecodedFrame::ClearLink { syn } => {
                active.selector.strategy_mut().probe.on_clear_link(syn)?;
                RammuxProgress::Empty
            },

            DecodedFrame::SessionWindowUpdate { update } => {
                let global = active.selector.strategy_mut();
                let transit = global
                    .transit_send
                    .as_mut()
                    .ok_or(ErrorKind::Transit("transit window is not enabled"))?;
                transit.credit = transit
                    .credit
                    .checked_add(update)
                    .ok_or(ErrorKind::Transit("transit window overflow"))?;
                if update > 0 {
                    // The grant is what the sender was stalled on, so the
                    // stall ends here rather than on the next outbound pass.
                    global.transit_blocked = false;
                    global.note_outbound_pass(false);
                    // Streams gated on transit credit returned Pending without
                    // a stream-level wakeup source, so wake all of them.
                    active.selector.wake_all();
                }
                RammuxProgress::Empty
            },

            DecodedFrame::Ping {
                payload,
                is_response,
            } => {
                let done = if is_response {
                    active.selector.strategy_mut().probe.on_pong(payload)?
                } else {
                    active.selector.strategy_mut().probe.on_ping(payload)?
                };
                let global = active.selector.strategy_mut();
                // Loaded-RTT samples come from the plain ping and from the
                // probe's own CLEAR_LINK leg, so they are drained here
                // rather than carried on probe completion.
                if let Some(dirty_rtt) = global.probe.take_dirty_rtt() {
                    global.dirty_rtt = Some(dirty_rtt);
                }
                if let Some(ProbeDone { rtt, resume }) = done {
                    global.rtt = Some(rtt);
                    if resume {
                        active.selector.wake_all();
                    }
                }
                RammuxProgress::Empty
            },

            DecodedFrame::Stream {
                stream_id,
                flags,
                payload,
            } if stream_id.initiated_by() == self.role => {
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

                let fin_state = match payload {
                    StreamPayload::WindowUpdate(update) => {
                        e.received_window_update(update, flags.fin_read, flags.fin_write)
                    },
                    StreamPayload::Data(data) => {
                        e.received_data(data, flags.fin_read, flags.fin_write)
                    },
                }
                .map_err(|error| ErrorKind::Stream {
                    id: stream_id,
                    error,
                })?;
                if fin_state.is_dead() {
                    active.streams.outbound.remove(slab_idx);
                }

                RammuxProgress::Empty
            },

            DecodedFrame::Stream {
                stream_id,
                flags,
                payload,
            } => {
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
                                error:
                                    "started a new stream without respecting the configured limit"
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

                let fin_state = match payload {
                    StreamPayload::WindowUpdate(update) => {
                        e.get_mut()
                            .received_window_update(update, flags.fin_read, flags.fin_write)
                    },
                    StreamPayload::Data(data) => {
                        e.get_mut()
                            .received_data(data, flags.fin_read, flags.fin_write)
                    },
                }
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
            },

            DecodedFrame::Terminate => {
                let downgraded = self.state.downgrade(true)?;
                RammuxProgress::Downgraded(downgraded)
            },
        };

        Poll::Ready(Ok(progress))
    }

    fn make_outbound_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Infallible, ErrorKind>> {
        let active = self.state.active_mut()?;

        loop {
            match active.codec.poll_ready_unpin(cx) {
                // The transport itself is backed up: whatever the sender is
                // waiting on, it is not a transit credit grant.
                Poll::Pending => {
                    active.selector.strategy_mut().note_outbound_pass(false);
                    return Poll::Pending;
                },
                Poll::Ready(result) => result?,
            }

            if let Some((frame, resume)) = active.selector.strategy_mut().probe.next_frame() {
                let item = match frame {
                    ProbeFrame::Clear(syn) => EncoderItem::new_clear_link(syn),
                    ProbeFrame::Ping(payload) => EncoderItem::new_ping(payload, false),
                    ProbeFrame::Pong(payload) => EncoderItem::new_ping(payload, true),
                };
                active.codec.start_send_unpin(item)?;
                if resume {
                    active.selector.wake_all();
                }
                continue;
            }

            if active.selector.strategy().probe_paused() {
                // Only probe and ping frames may travel while clearing the
                // link; flush and wait. The probe pause is its own cost and
                // must not be charged to the transit window.
                active.selector.strategy_mut().note_outbound_pass(false);
                let _ = active.codec.poll_flush_unpin(cx)?;
                break Poll::Pending;
            }

            if let Some(update) = active.selector.strategy_mut().transit_recv_update() {
                active
                    .codec
                    .start_send_unpin(EncoderItem::new_session_window_update(update))?;
                continue;
            }

            if let Poll::Ready(Some((update, fin_state))) = active.selector.poll_next_unpin(cx) {
                if update.data.is_empty().not() {
                    let global = active.selector.strategy_mut();
                    global.transit_blocked = false;
                    global.note_outbound_pass(false);
                }
                let id = update.id;
                let item = EncoderItem::from(update);
                active.codec.start_send_unpin(item)?;
                if fin_state.is_dead() {
                    if id.initiated_by() == self.role {
                        let idx = id.slab_idx();
                        active.streams.outbound.remove(idx);
                    } else {
                        active.streams.inbound.remove(&id);
                    }
                }
                continue;
            } else {
                // Every stream came back pending. If at least one of them had
                // payload ready and only the spent transit window held it
                // back, the sender is stalled on a credit grant.
                let blocked = active.selector.strategy().transit_blocked;
                active.selector.strategy_mut().note_outbound_pass(blocked);
                let _ = active.codec.poll_flush_unpin(cx)?;
                break Poll::Pending;
            }
        }
    }

    fn poll_progress_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, ErrorKind>> {
        let mut inbound = Poll::Pending;
        // Drain up to one encoder batch of inbound frames first.
        // In particular, processing multiple WINDOW_UPDATEs can make several
        // outbound streams writable, allowing their frames to be coalesced into
        // a vectored write. Keep the work bounded so ready reads cannot starve writes.
        for _ in 0..codec::ENCODER_QUEUE_CAPACITY {
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
        // Both halves of the pass produce probe transitions - an inbound
        // pong completes an exchange, an outbound frame starts one - so
        // they are drained once, at the end. A new stream outranks them:
        // it is reported now, and the events keep until the next poll.
        if let Poll::Ready(progress @ RammuxProgress::Inbound(..)) = inbound {
            return Poll::Ready(Ok(progress));
        }
        match self
            .state
            .active_mut()?
            .selector
            .strategy_mut()
            .probe
            .next_event()
        {
            Some(event) => Poll::Ready(Ok(RammuxProgress::Probe(event))),
            None => inbound.map(Ok),
        }
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
            Ok(RammuxProgress::Empty | RammuxProgress::Inbound(..) | RammuxProgress::Probe(..)) => {
            },
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

    /// Initiates a link-clearing probe, measuring the clean RTT.
    ///
    /// Both sides pause data output, drain the link with a `CLEAR_LINK`
    /// exchange, and time a `PING` over it. The sample is the path's own
    /// cost, which this connection's standing queues cannot inflate; the
    /// session-level transit window is sized from it.
    ///
    /// Returns whether the probe was started. It is refused (`false`)
    /// while another probe is running - including one the peer initiated -
    /// and while a plain [`Self::send_ping`] is unanswered, because that
    /// ping's pong would be indistinguishable from the probe's own. Either
    /// retry later or give the ping up with [`Self::abandon_ping`].
    ///
    /// The `CLEAR_LINK` is encoded on the next poll, not here. Nothing in
    /// this crate gives up on the probe: data output stays paused until it
    /// completes, so an application that wants a liveness check must put
    /// its own deadline on the [`ProbeEvent::ProbeStarted`] that this
    /// produces.
    pub fn start_probe(&mut self) -> Result<bool, RammuxError> {
        Ok(self
            .state
            .active_mut()?
            .selector
            .strategy_mut()
            .probe
            .start())
    }

    /// Sends a plain `PING`, measuring the loaded RTT.
    ///
    /// The ping travels inline with data, so it times the round trip
    /// through the queues that are actually standing. Per-stream receive
    /// windows are sized from it, because a stream's credit loop runs
    /// through those same queues.
    ///
    /// Returns whether the ping was queued. It is refused (`false`) while
    /// a probe is running, since a `PING` there belongs to the dance, and
    /// while another plain ping is still outstanding.
    ///
    /// The frame is encoded on the next poll, not here, and nothing in
    /// this crate gives up on it: an application that wants a deadline
    /// must put it on the [`ProbeEvent::PingSent`] that this produces, and
    /// call [`Self::abandon_ping`] when it expires.
    pub fn send_ping(&mut self) -> Result<bool, RammuxError> {
        Ok(self
            .state
            .active_mut()?
            .selector
            .strategy_mut()
            .probe
            .send_ping())
    }

    /// Gives up on the outstanding plain `PING`, freeing
    /// [`Self::start_probe`] to run.
    ///
    /// Returns whether one was in flight. A `PONG` that arrives for it
    /// afterwards is ignored rather than failing the connection.
    pub fn abandon_ping(&mut self) -> Result<bool, RammuxError> {
        Ok(self
            .state
            .active_mut()?
            .selector
            .strategy_mut()
            .probe
            .abandon_ping())
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
        let (transit_starved, transit_starved_events) = self
            .state
            .active()
            .map(|active| {
                let global = active.selector.strategy();
                let pending = global
                    .stalled_since
                    .map(|at| at.elapsed())
                    .unwrap_or_default();
                (global.stalled_total + pending, global.stalled_events)
            })
            .unwrap_or_default();
        let dirty_rtt = self
            .state
            .active()
            .ok()
            .and_then(|active| active.selector.strategy().dirty_rtt);
        let (transit_send_credit, transit_recv_window) = self
            .state
            .active()
            .map(|active| {
                let global = active.selector.strategy();
                (
                    global.transit_send.as_ref().map(|transit| transit.credit),
                    global.transit_recv.as_ref().map(|recv| recv.current),
                )
            })
            .unwrap_or_default();

        RammuxStats {
            inbound_streams,
            outbound_streams,
            rtt,
            available_global_recv_window,
            transit_send_credit,
            transit_recv_window,
            transit_starved,
            transit_starved_events,
            dirty_rtt,
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
    /// The RTT-measurement state machine changed state.
    ///
    /// See [`ProbeEvent`] doc for more info. An application that does not
    /// schedule pings or probes, and does not want a liveness check, can
    /// ignore this variant entirely.
    Probe(ProbeEvent),
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
    /// Most recent clean round trip time, measured by the link-clearing
    /// probe over the drained link.
    ///
    /// Empty until the first probe completes. Probes are started with
    /// [`RammuxConnection::start_probe`], so a connection that never
    /// probes never fills this in.
    pub rtt: Option<Duration>,
    /// Bytes available in the global receive window pool.
    pub available_global_recv_window: usize,
    /// Most recent loaded round trip time: the path plus both sides'
    /// standing queues, sampled by [`RammuxConnection::send_ping`] and by
    /// the probe's own `CLEAR_LINK` leg.
    pub dirty_rtt: Option<Duration>,
    /// Remaining in-flight credit granted to us by the peer, if the transit window is enabled.
    pub transit_send_credit: Option<u32>,
    /// Current size of the transit window we grant to the peer, if enabled.
    pub transit_recv_window: Option<u32>,
    /// Total time the sender spent stalled on a transit credit grant: the
    /// transport was writable and stream payload was ready, but the transit
    /// window was spent.
    pub transit_starved: Duration,
    /// How many such stalls the sender entered.
    pub transit_starved_events: u64,
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroU32, time::Duration};

    use bytes::Bytes;
    use futures::{FutureExt, SinkExt, StreamExt, stream::FuturesUnordered};
    use rstest::rstest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use crate::{
        config::{RammuxConfig, RammuxRole},
        connection::{ProbeEvent, RammuxConnection, RammuxProgress},
        stream::RammuxDuplex,
    };

    const DATA: Bytes = Bytes::from_static(&[b'A'; 64 * 1024]);

    #[rstest]
    #[tokio::test]
    async fn two_sides(#[values(1, 16, 64)] streams: u32) {
        let (io_1, io_2) = tokio::io::duplex(512);
        let mut config = RammuxConfig::new();
        config.frame_limit = NonZeroU32::new(256).unwrap();
        config.local_recv_window = NonZeroU32::new(1024).unwrap();
        config.remote_recv_window = 1024;
        config.max_inbound_streams = 16;
        config.max_outbound_streams = 16;
        let conn_1 = RammuxConnection::new(RammuxRole::Client, io_1, config.clone());
        let conn_2 = RammuxConnection::new(RammuxRole::Server, io_2, config.clone());
        tokio::join!(
            run_rammux(conn_1, streams, 32 * 1024).then(verify_io_clean),
            run_rammux(conn_2, streams, 32 * 1024).then(verify_io_clean),
        );
    }

    /// Runs a connection with the RTT schedule an application is expected
    /// to impose on it: probes and plain pings alongside the stream IO.
    async fn run_rammux(
        mut conn: RammuxConnection<DuplexStream>,
        streams: u32,
        data_in_stream: usize,
    ) -> DuplexStream {
        let mut remaining_inbound = streams;
        let mut remaining_outbound = streams;
        let mut remaining_finished = streams * 2;
        let mut futs = FuturesUnordered::new();
        let mut probes = tokio::time::interval(Duration::from_millis(25));
        let mut pings = tokio::time::interval(Duration::from_millis(7));

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
                _ = probes.tick() => {
                    // An unanswered ping blocks the probe; give it up so
                    // the next tick can go through.
                    if !conn.start_probe().unwrap() {
                        conn.abandon_ping().unwrap();
                    }
                    continue;
                }
                _ = pings.tick() => {
                    conn.send_ping().unwrap();
                    continue;
                }
                progress = conn.progress() => progress,
            };
            match progress.unwrap() {
                RammuxProgress::Empty | RammuxProgress::Probe(..) => {},
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

    /// Polls both sides until `want` transitions have been reported by
    /// `a`, and returns them.
    async fn exchange(
        a: &mut RammuxConnection<DuplexStream>,
        b: &mut RammuxConnection<DuplexStream>,
        want: usize,
    ) -> Vec<ProbeEvent> {
        let collect = async {
            let mut events = Vec::new();
            while events.len() < want {
                tokio::select! {
                    progress = a.progress() => {
                        if let RammuxProgress::Probe(event) = progress.unwrap() {
                            events.push(event);
                        }
                    }
                    progress = b.progress() => {
                        progress.unwrap();
                    }
                }
            }
            events
        };
        tokio::time::timeout(Duration::from_secs(5), collect)
            .await
            .expect("the exchange never completed")
    }

    /// Neither `PING` mechanism runs on its own, and each reports the
    /// transitions an application needs to time it out.
    #[tokio::test]
    async fn ping_and_probe_are_caller_driven() {
        let (io_1, io_2) = tokio::io::duplex(4096);
        let config = RammuxConfig::new();
        let mut client = RammuxConnection::new(RammuxRole::Client, io_1, config.clone());
        let mut server = RammuxConnection::new(RammuxRole::Server, io_2, config);

        // An idle connection measures nothing: no clock, no samples.
        assert!(client.stats().rtt.is_none());
        assert!(client.stats().dirty_rtt.is_none());

        // A plain ping, in and out, yields the loaded sample.
        assert!(client.send_ping().unwrap());
        assert!(!client.send_ping().unwrap(), "only one is outstanding");
        let events = exchange(&mut client, &mut server, 2).await;
        assert_eq!(events[0], ProbeEvent::PingSent);
        assert!(matches!(events[1], ProbeEvent::PingAnswered { .. }));
        assert!(client.stats().dirty_rtt.is_some());
        assert!(
            client.stats().rtt.is_none(),
            "a plain ping is not a clean sample"
        );

        // A probe yields the clean sample the transit autotune sizes from.
        assert!(client.start_probe().unwrap());
        assert!(!client.start_probe().unwrap(), "one probe at a time");
        let events = exchange(&mut client, &mut server, 2).await;
        assert_eq!(events[0], ProbeEvent::ProbeStarted { initiated: true });
        assert!(matches!(events[1], ProbeEvent::ProbeCompleted { .. }));
        assert!(client.stats().rtt.is_some());
    }

    /// The peer's probe is announced too: it pauses this side's data, so
    /// an application watching for a stuck connection has to see it.
    #[tokio::test]
    async fn a_peer_probe_is_reported() {
        let (io_1, io_2) = tokio::io::duplex(4096);
        let config = RammuxConfig::new();
        let mut client = RammuxConnection::new(RammuxRole::Client, io_1, config.clone());
        let mut server = RammuxConnection::new(RammuxRole::Server, io_2, config);

        assert!(client.start_probe().unwrap());
        let events = exchange(&mut server, &mut client, 2).await;
        assert_eq!(events[0], ProbeEvent::ProbeStarted { initiated: false });
        assert!(matches!(events[1], ProbeEvent::ProbeCompleted { .. }));
        assert!(server.stats().rtt.is_some());
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
