//! Types for running rammux protocol on an IO transport.

use std::{
    collections::hash_map::Entry,
    convert::Infallible,
    ops::Not,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::selector::Selector;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    codec,
    config::{RammuxConfig, RammuxRole},
    connection::state::{Active, ConnState},
    error::{ErrorKind, RammuxError, StreamError},
    global_pool::GlobalPool,
    stream::{FinState, RammuxDuplex, handle::StreamHandle},
    stream_id::StreamId,
    transport::{StreamFrame, Transport},
};

pub use crate::{connection::downgrade::Downgraded, transport::ProbeEvent};

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
/// A fresh connection has one exchange already under way: the link is
/// clear before either side has sent anything, so the opening `PING`
/// costs one round trip and needs no draining.
///
/// Neither mechanism is given up on. Every transition is reported as a
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
                transport: Transport::new(role, io, &config),
                streams: Default::default(),
                selector: Selector::new(GlobalPool {
                    available: config.global_recv_window,
                    ..Default::default()
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

    /// Routes one inbound stream frame to the stream it belongs to.
    fn poll_inbound_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RammuxProgress<IO>, RammuxError>> {
        let active = self.state.active_mut()?;
        let frame = match std::task::ready!(active.transport.poll_next_unpin(cx)) {
            Some(frame) => frame?,
            // The peer's TERM: the connection is over and the transport
            // goes back to the application.
            None => {
                let downgraded = self.state.downgrade(true)?;
                return Poll::Ready(Ok(RammuxProgress::Downgraded(downgraded)));
            },
        };

        let stream_id = frame.stream_id();
        let flags = frame.flags();
        let to_stream = |handle: &mut StreamHandle| -> Result<FinState, StreamError> {
            match frame {
                StreamFrame::Data(data) => {
                    handle.received_data(data.payload, flags.fin_read, flags.fin_write)
                },
                StreamFrame::WindowUpdate(update) => {
                    handle.received_window_update(update.update, flags.fin_read, flags.fin_write)
                },
            }
        };

        // Streams we started ourselves: the peer may only ever answer on
        // one, never open it.
        if stream_id.initiated_by() == self.role {
            if flags.syn {
                return Poll::Ready(Err(ErrorKind::Stream {
                    id: stream_id,
                    error: "started a new stream with an ID from the wrong pool".into(),
                }
                .into()));
            }
            let slab_idx = stream_id.slab_idx();
            let handle = active
                .streams
                .outbound
                .get_mut(slab_idx)
                .ok_or(ErrorKind::Stream {
                    id: stream_id,
                    error: "sent a frame for an unknown stream".into(),
                })?;
            let fin_state = to_stream(handle).map_err(|error| ErrorKind::Stream {
                id: stream_id,
                error,
            })?;
            if fin_state.is_dead() {
                active.streams.outbound.remove(slab_idx);
            }
            return Poll::Ready(Ok(RammuxProgress::Empty));
        }

        let stream_count = active.streams.inbound.len();
        let (mut e, new_stream) = match active.streams.inbound.entry(stream_id) {
            Entry::Occupied(..) if flags.syn => {
                return Poll::Ready(Err(ErrorKind::Stream {
                    id: stream_id,
                    error: "started a new stream with an occupied ID".into(),
                }
                .into()));
            },
            Entry::Occupied(e) => (e, None),
            Entry::Vacant(e) if flags.syn => {
                if stream_count == crate::safe_cast_usize(self.config.max_inbound_streams) {
                    return Poll::Ready(Err(ErrorKind::Stream {
                        id: stream_id,
                        error: "started a new stream without respecting the configured limit"
                            .into(),
                    }
                    .into()));
                }
                let (handle, updates, duplex) = crate::stream::new(stream_id, false, &self.config);
                active.selector.push(updates);
                (e.insert_entry(handle), Some(duplex))
            },
            Entry::Vacant(..) => {
                return Poll::Ready(Err(ErrorKind::Stream {
                    id: stream_id,
                    error: "sent a frame for an unknown stream".into(),
                }
                .into()));
            },
        };

        let fin_state = to_stream(e.get_mut()).map_err(|error| ErrorKind::Stream {
            id: stream_id,
            error,
        })?;
        if fin_state.is_dead() {
            e.remove();
        }

        Poll::Ready(Ok(new_stream
            .map(RammuxProgress::Inbound)
            .unwrap_or(RammuxProgress::Empty)))
    }

    /// Feeds the transport stream frames for as long as it takes them.
    fn make_outbound_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Infallible, RammuxError>> {
        let active = self.state.active_mut()?;

        loop {
            match active.transport.poll_ready_unpin(cx) {
                // The transport is holding everything back - a probe owns
                // the link, or the IO itself is backed up. Either way the
                // sender is not waiting on a transit credit grant.
                Poll::Pending => {
                    active.transport.note_send_pass(false);
                    let _ = active.transport.poll_flush_unpin(cx)?;
                    return Poll::Pending;
                },
                Poll::Ready(result) => result?,
            }

            // What the streams need to see of the transport, refreshed
            // before every pass over them.
            let budget = active.transport.send_budget();
            let dirty_rtt = active.transport.dirty_rtt().map(|rtt| rtt.value);
            let global = active.selector.strategy_mut();
            global.send_budget = budget;
            global.dirty_rtt = dirty_rtt;

            let Poll::Ready(Some((frame, fin_state))) = active.selector.poll_next_unpin(cx) else {
                // Every stream came back pending. If at least one of them
                // had payload ready and only the spent transit window
                // held it back, the sender is stalled on a credit grant.
                let blocked = active.selector.strategy().transit_blocked;
                active.transport.note_send_pass(blocked);
                let _ = active.transport.poll_flush_unpin(cx)?;
                return Poll::Pending;
            };

            if let StreamFrame::Data(data) = &frame
                && data.payload.is_empty().not()
            {
                active.selector.strategy_mut().transit_blocked = false;
            }
            let id = frame.stream_id();
            active.transport.start_send_unpin(frame)?;
            if fin_state.is_dead() {
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
    ) -> Poll<Result<RammuxProgress<IO>, RammuxError>> {
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

        // A finished probe or an arrived credit grant unblocks senders
        // that parked on wakers only the selector holds.
        let active = self.state.active_mut()?;
        if active.transport.take_unblocked() {
            active.selector.strategy_mut().transit_blocked = false;
            active.selector.wake_all();
        }

        let _ = self.make_outbound_progress(cx)?;

        // Both halves of the pass produce probe transitions - an inbound
        // pong completes an exchange, an outbound frame starts one - so
        // they are drained once, at the end. A new stream outranks them:
        // it is reported now, and the events keep until the next poll.
        if let Poll::Ready(progress @ RammuxProgress::Inbound(..)) = inbound {
            return Poll::Ready(Ok(progress));
        }
        match self.state.active_mut()?.transport.next_event() {
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
        if let Err(error) = &result
            && error.is_user().not()
        {
            self.state = ConnState::Poisoned;
        }
        Poll::Ready(result)
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
    /// while another probe is running - including one the peer initiated,
    /// and the opening exchange - and after the peer has terminated the
    /// connection. A plain ping in flight does not refuse it: the probe
    /// takes the link and gives that ping up.
    ///
    /// The `CLEAR_LINK` is encoded on the next poll, not here. Nothing in
    /// this crate gives up on the probe: data output stays paused until it
    /// completes, so an application that wants a liveness check must put
    /// its own deadline on the [`ProbeEvent::ProbeStarted`] that this
    /// produces.
    pub fn start_probe(&mut self) -> Result<bool, RammuxError> {
        Ok(self.state.active_mut()?.transport.init_probe())
    }

    /// Sends a plain `PING`, measuring the loaded RTT.
    ///
    /// The ping travels inline with data, so it times the round trip
    /// through the queues that are actually standing. Per-stream receive
    /// windows are sized from it, because a stream's credit loop runs
    /// through those same queues.
    ///
    /// Returns whether the ping was queued. It is refused (`false`) while
    /// a probe is running, since a `PING` there belongs to the dance,
    /// while another plain ping is still outstanding, and after the peer
    /// has terminated the connection.
    ///
    /// The frame is encoded on the next poll, not here, and nothing in
    /// this crate gives up on it: an application that wants a deadline
    /// must put it on the [`ProbeEvent::PingSent`] that this produces, and
    /// call [`Self::abandon_ping`] when it expires.
    pub fn send_ping(&mut self) -> Result<bool, RammuxError> {
        Ok(self.state.active_mut()?.transport.init_ping())
    }

    /// Gives up on the outstanding plain `PING`, freeing [`Self::send_ping`]
    /// to run again.
    ///
    /// Returns whether one was in flight. A `PONG` that arrives for it
    /// afterwards is ignored rather than failing the connection.
    pub fn abandon_ping(&mut self) -> Result<bool, RammuxError> {
        Ok(self.state.active_mut()?.transport.abandon_ping())
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
        let (transit_starved, transit_starved_events) = active.transport.transit_stalls();
        RammuxStats {
            inbound_streams: u32::try_from(active.streams.inbound.len())
                .expect("we can't have more than u32 inbound streams"),
            outbound_streams: u32::try_from(active.streams.outbound.len())
                .expect("we can't have more than u32 outbound streams"),
            rtt: active.transport.clean_rtt().map(|rtt| rtt.value),
            dirty_rtt: active.transport.dirty_rtt().map(|rtt| rtt.value),
            available_global_recv_window: active.selector.strategy().available,
            transit_send_credit: active.transport.send_budget(),
            transit_recv_window: active.transport.transit_window(),
            transit_starved,
            transit_starved_events,
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
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RammuxStats {
    /// Count of currently active inbound streams.
    pub inbound_streams: u32,
    /// Count of currently active outbound streams.
    pub outbound_streams: u32,
    /// Most recent clean round trip time, measured by the link-clearing
    /// probe over the drained link.
    ///
    /// Empty until the opening exchange completes. Later samples come
    /// from [`RammuxConnection::start_probe`], so a connection that never
    /// probes keeps the opening one.
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

    /// Polls both sides until each has reported the transitions asked of
    /// it, and returns what each reported.
    async fn exchange(
        a: &mut RammuxConnection<DuplexStream>,
        b: &mut RammuxConnection<DuplexStream>,
        want_a: usize,
        want_b: usize,
    ) -> (Vec<ProbeEvent>, Vec<ProbeEvent>) {
        let collect = async {
            let (mut events_a, mut events_b) = (Vec::new(), Vec::new());
            while events_a.len() < want_a || events_b.len() < want_b {
                tokio::select! {
                    progress = a.progress() => {
                        if let RammuxProgress::Probe(event) = progress.unwrap() {
                            events_a.push(event);
                        }
                    }
                    progress = b.progress() => {
                        if let RammuxProgress::Probe(event) = progress.unwrap() {
                            events_b.push(event);
                        }
                    }
                }
            }
            (events_a, events_b)
        };
        tokio::time::timeout(Duration::from_secs(5), collect)
            .await
            .expect("the exchange never completed")
    }

    fn pair() -> (
        RammuxConnection<DuplexStream>,
        RammuxConnection<DuplexStream>,
    ) {
        let (io_1, io_2) = tokio::io::duplex(4096);
        let config = RammuxConfig::new();
        (
            RammuxConnection::new(RammuxRole::Client, io_1, config.clone()),
            RammuxConnection::new(RammuxRole::Server, io_2, config),
        )
    }

    /// The link is clear before either side has sent anything, so the
    /// first sample costs one round trip and needs no draining.
    #[tokio::test]
    async fn a_fresh_connection_measures_itself() {
        let (mut client, mut server) = pair();
        assert!(client.stats().rtt.is_none());
        assert!(client.stats().dirty_rtt.is_none());

        let (client_events, server_events) = exchange(&mut client, &mut server, 2, 2).await;
        for events in [&client_events, &server_events] {
            assert_eq!(events[0], ProbeEvent::ProbeStarted { initiated: true });
            assert!(matches!(events[1], ProbeEvent::ProbeCompleted { .. }));
        }
        assert!(client.stats().rtt.is_some());
        assert!(
            client.stats().dirty_rtt.is_some(),
            "on an empty link the clean sample is also the loaded one"
        );
    }

    /// Neither `PING` mechanism runs on its own, and each reports the
    /// transitions an application needs to time it out.
    #[tokio::test]
    async fn ping_and_probe_are_caller_driven() {
        let (mut client, mut server) = pair();
        assert!(
            !client.send_ping().unwrap(),
            "the opening exchange owns the link"
        );
        exchange(&mut client, &mut server, 2, 2).await;
        let clean = client.stats().rtt;

        // A plain ping, in and out, yields the loaded sample.
        assert!(client.send_ping().unwrap());
        assert!(!client.send_ping().unwrap(), "only one is outstanding");
        let (events, ..) = exchange(&mut client, &mut server, 2, 0).await;
        assert_eq!(events[0], ProbeEvent::PingSent);
        assert!(matches!(events[1], ProbeEvent::PingAnswered { .. }));
        assert_eq!(
            client.stats().rtt,
            clean,
            "a plain ping is not a clean sample"
        );

        // A probe yields the clean sample the transit autotune sizes from.
        assert!(client.start_probe().unwrap());
        assert!(!client.start_probe().unwrap(), "one probe at a time");
        let (client_events, server_events) = exchange(&mut client, &mut server, 2, 2).await;
        assert_eq!(
            client_events[0],
            ProbeEvent::ProbeStarted { initiated: true }
        );
        assert!(matches!(
            client_events[1],
            ProbeEvent::ProbeCompleted { .. }
        ));
        // The peer's probe is announced there too: it pauses that side's
        // data, so an application watching for a stuck connection sees it.
        assert_eq!(
            server_events[0],
            ProbeEvent::ProbeStarted { initiated: false }
        );
        assert!(matches!(
            server_events[1],
            ProbeEvent::ProbeCompleted { .. }
        ));
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
