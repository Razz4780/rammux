//! Types for running rammux protocol on an IO transport.

use std::{
    collections::hash_map::Entry,
    convert::Infallible,
    io,
    task::{Context, Poll},
    time::Duration,
};

use async_selector::selector::Selector;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use transit::Transit;

use crate::{
    codec::{
        self, RammuxCodec,
        decoder::{DecodedFrame, StreamPayload},
        encoder::EncoderItem,
    },
    config::{RammuxConfig, RammuxRole},
    connection::state::{Active, ConnState},
    error::{ErrorKind, RammuxError},
    global_pool::GlobalPool,
    ping::{Ping, PingFrame},
    stream::RammuxDuplex,
    stream_id::StreamId,
};

pub use crate::{connection::downgrade::Downgraded, ping::PingEvent};

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
/// 5. surface `PING` transitions as [`RammuxProgress::Ping`].
///
/// If the connection stops being polled, stream IO stalls, flow-control updates
/// stop, and closed stream IDs are not reclaimed.
///
/// # Transit window
///
/// rammux does not write to the IO transport directly. The transport is
/// wrapped in a [`transit::Transit`], which bounds how much data is in flight
/// between the peers and steers that bound from the queuing delay it
/// observes, so that a bulk stream cannot fill the path and delay everything
/// sharing it. Both peers do this - `Transit` is a framed protocol of its
/// own, not a transparent shim - and it is not optional. Its settings are
/// [`RammuxConfig::transit_sizing`] and [`RammuxConfig::transit_probe_spacing`];
/// the [`RammuxRole::Client`] is the transit
/// [`Initiator`](transit::Role::Initiator), which pays for the link-clearing
/// probe both ends size from. What that layer is doing is reported in
/// [`RammuxStats::transit`].
///
/// Two settings on the transport itself matter to that layer and are the
/// application's to get right: `TCP_NODELAY` on both ends, and a send
/// buffer that can grow past the transit window. See
/// [`transit::connection`].
///
/// # RTT measurement
///
/// A connection runs no timers of its own. Nothing here sleeps, and the
/// only thing that ever wakes the connection's task is the transport. The
/// `PING` mechanism is therefore started by your code:
/// [`RammuxConnection::send_ping`] measures the *loaded* RTT with a `PING`
/// that travels inline with data, and stream receive windows are sized
/// from it. Nothing in this crate gives up on it. Every transition is
/// reported as a [`RammuxProgress::Ping`], which is what lets your code
/// impose the schedule and the deadline it wants, and
/// [`RammuxConnection::abandon_ping`] is how it enforces one.
///
/// The *clean* RTT, over a drained link, is measured by the transit layer
/// on its own schedule and needs no driving.
///
/// # Downgrade
///
/// To stop using rammux and recover the wrapped transport, call
/// [`RammuxConnection::downgrade`] and await the returned [`Downgraded`].
/// That future sends the final `TERM` frame, waits for the peer's `TERM`,
/// and yields the transit layer over the original transport, with no
/// unread rammux bytes left in it.
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
    /// # Panics
    ///
    /// Must be called from within a Tokio runtime context: the transit layer
    /// arms a timer for its probe schedule here.
    pub fn new(role: RammuxRole, io: IO, config: RammuxConfig) -> Self {
        let transit = Transit::new(io, config.transit_config(role));
        Self {
            state: ConnState::Active(Active {
                codec: RammuxCodec::new(transit, config.frame_limit),
                streams: Default::default(),
                selector: Selector::new(GlobalPool {
                    rtt: None,
                    available: config.global_recv_window,
                    ping: Ping::new(),
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

        let progress = match frame {
            DecodedFrame::Ping {
                payload,
                is_response,
            } => {
                let global = active.selector.strategy_mut();
                if is_response {
                    if let Some(rtt) = global.ping.on_pong(payload)? {
                        global.rtt = Some(rtt);
                    }
                } else {
                    global.ping.on_ping(payload);
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
            std::task::ready!(active.codec.poll_ready_unpin(cx))?;

            // Pings first: the peer is timing the pongs, and our own ping
            // is timing the queue it has to wait in, so neither should
            // wait behind a round of stream frames.
            if let Some(frame) = active.selector.strategy_mut().ping.next_frame() {
                let item = match frame {
                    PingFrame::Ping(payload) => EncoderItem::new_ping(payload, false),
                    PingFrame::Pong(payload) => EncoderItem::new_ping(payload, true),
                };
                active.codec.start_send_unpin(item)?;
                continue;
            }

            if let Poll::Ready(Some((update, fin_state))) = active.selector.poll_next_unpin(cx) {
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
        // Both halves of the pass produce ping transitions - an inbound
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
            .ping
            .next_event()
        {
            Some(event) => Poll::Ready(Ok(RammuxProgress::Ping(event))),
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
            Ok(RammuxProgress::Empty | RammuxProgress::Inbound(..) | RammuxProgress::Ping(..)) => {
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

    /// Sends a `PING`, measuring the loaded RTT.
    ///
    /// The ping travels inline with data, so it times the round trip
    /// through the queues that are actually standing - the transit layer's
    /// credit wait included. Per-stream receive windows are sized from it,
    /// because a stream's credit loop runs through those same queues.
    ///
    /// Returns whether the ping was queued. It is refused (`false`) while
    /// another ping is still outstanding: only one is ever in flight, so
    /// its pong is unambiguous.
    ///
    /// The frame is encoded on the next poll, not here, and nothing in
    /// this crate gives up on it: an application that wants a deadline
    /// must put it on the [`PingEvent::Sent`] that this produces, and
    /// call [`Self::abandon_ping`] when it expires.
    pub fn send_ping(&mut self) -> Result<bool, RammuxError> {
        Ok(self.state.active_mut()?.selector.strategy_mut().ping.send())
    }

    /// Gives up on the outstanding `PING`, freeing [`Self::send_ping`] to
    /// run again.
    ///
    /// Returns whether one was in flight. A `PONG` that arrives for it
    /// afterwards is ignored rather than failing the connection.
    pub fn abandon_ping(&mut self) -> Result<bool, RammuxError> {
        Ok(self
            .state
            .active_mut()?
            .selector
            .strategy_mut()
            .ping
            .abandon())
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
        let active = self.state.active().ok();
        let global = active.map(|active| active.selector.strategy());
        RammuxStats {
            inbound_streams: active.map_or(0, |active| {
                u32::try_from(active.streams.inbound.len())
                    .expect("we can't have more than u32 inbound streams")
            }),
            outbound_streams: active.map_or(0, |active| {
                u32::try_from(active.streams.outbound.len())
                    .expect("we can't have more than u32 outbound streams")
            }),
            rtt: global.and_then(|global| global.rtt),
            available_global_recv_window: global.map_or(0, |global| global.available),
            transit: active.map(|active| active.codec.io().stats()),
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
    /// The connection's `PING` exchange changed state.
    ///
    /// See [`PingEvent`] doc for more info. An application that does not
    /// send pings can ignore this variant entirely.
    Ping(PingEvent),
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
    /// Most recent loaded round trip time: the path plus both sides'
    /// standing queues, sampled by [`RammuxConnection::send_ping`].
    ///
    /// Empty until the first pong arrives. Pings are started by the
    /// application, so a connection that never pings never fills this in.
    pub rtt: Option<Duration>,
    /// Bytes available in the global receive window pool.
    pub available_global_recv_window: usize,
    /// What the transit layer says about itself: the window it grants the
    /// peer, the credit the peer grants us, the clean round trip its probe
    /// measured, and how often the sender ran out of credit.
    ///
    /// Empty once the connection is downgraded or poisoned.
    pub transit: Option<transit::Stats>,
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroU32, time::Duration};

    use bytes::Bytes;
    use futures::{FutureExt, SinkExt, StreamExt, stream::FuturesUnordered};
    use rstest::rstest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use transit::Transit;

    use crate::{
        config::{RammuxConfig, RammuxRole},
        connection::{PingEvent, RammuxConnection, RammuxProgress},
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

    /// Runs a connection with the ping schedule an application is expected
    /// to impose on it, alongside the stream IO.
    async fn run_rammux(
        mut conn: RammuxConnection<DuplexStream>,
        streams: u32,
        data_in_stream: usize,
    ) -> Transit<DuplexStream> {
        let mut remaining_inbound = streams;
        let mut remaining_outbound = streams;
        let mut remaining_finished = streams * 2;
        let mut futs = FuturesUnordered::new();
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
                _ = pings.tick() => {
                    conn.send_ping().unwrap();
                    continue;
                }
                progress = conn.progress() => progress,
            };
            match progress.unwrap() {
                RammuxProgress::Empty | RammuxProgress::Ping(..) => {},
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
    ) -> Vec<PingEvent> {
        let collect = async {
            let mut events = Vec::new();
            while events.len() < want {
                tokio::select! {
                    progress = a.progress() => {
                        if let RammuxProgress::Ping(event) = progress.unwrap() {
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

    /// The ping does not run on its own, and it reports the transitions an
    /// application needs to time it out.
    #[tokio::test]
    async fn the_ping_is_caller_driven() {
        let (io_1, io_2) = tokio::io::duplex(4096);
        let config = RammuxConfig::new();
        let mut client = RammuxConnection::new(RammuxRole::Client, io_1, config.clone());
        let mut server = RammuxConnection::new(RammuxRole::Server, io_2, config);

        // An idle connection measures nothing: no clock, no samples.
        assert!(client.stats().rtt.is_none());

        assert!(client.send_ping().unwrap());
        assert!(!client.send_ping().unwrap(), "only one is outstanding");
        let events = exchange(&mut client, &mut server, 2).await;
        assert_eq!(events[0], PingEvent::Sent);
        assert!(matches!(events[1], PingEvent::Answered { .. }));
        assert!(client.stats().rtt.is_some());
        assert!(client.send_ping().unwrap(), "the next one is free to go");
    }

    /// The transit layer underneath is real: its probe runs on the client's
    /// schedule without anything driving it, and what it measures is
    /// reported through the connection's statistics.
    #[tokio::test]
    async fn the_transit_layer_probes_and_reports() {
        let (io_1, io_2) = tokio::io::duplex(4096);
        let config = RammuxConfig::new();
        let mut client = RammuxConnection::new(RammuxRole::Client, io_1, config.clone());
        let mut server = RammuxConnection::new(RammuxRole::Server, io_2, config);

        let before = client
            .stats()
            .transit
            .expect("an active connection has a transit layer");
        assert_eq!(before.window, client.config().transit_sizing.initial);
        assert_eq!(before.probes, 0);

        // The client's first probe is due at once - once the timer wheel
        // agrees, which over an in-memory pipe is later than the whole
        // exchange below takes. Its `CLEAR_LINK` then pauses the client's
        // output until the probe completes, so a ping cannot have come back
        // before the probe did.
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(client.send_ping().unwrap());
        exchange(&mut client, &mut server, 2).await;

        let client_transit = client.stats().transit.unwrap();
        assert!(client_transit.probes > 0, "no probe completed");
        assert!(client_transit.clean_rtt.is_some(), "no clean round trip");
        let server_transit = server.stats().transit.unwrap();
        assert_eq!(
            server_transit.probes, 0,
            "the server is the responder and initiates nothing"
        );
        assert!(
            server_transit.clean_rtt.is_some(),
            "the initiator's measurement never reached the responder"
        );
    }

    async fn verify_io_clean(io: Transit<DuplexStream>) {
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
