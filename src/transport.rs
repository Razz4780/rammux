//! The rammux transport: framing, RTT measurement and the transit window,
//! wrapped around an IO transport.
//!
//! [`Transport`] is the layer between raw bytes and stream multiplexing.
//! It turns an [`AsyncRead`] + [`AsyncWrite`] into a [`Stream`] and
//! [`Sink`] of [`StreamFrame`]s, and handles everything that is *not*
//! about any one stream on its own:
//!
//! - the session-level **transit window**, which bounds how much `DATA`
//!   payload may be in flight at once,
//! - the **plain ping**, which measures the loaded round trip,
//! - the **link-clearing probe**, which measures the round trip over a
//!   drained link.
//!
//! None of those appear in the frames it yields. A caller writes stream
//! frames and reads stream frames; the `PING`, `CLEAR_LINK` and
//! `SESSION_WINDOW_UPDATE` traffic that keeps the connection measured and
//! paced is generated, answered and consumed here.
//!
//! # No timers
//!
//! Nothing in this module sleeps or wakes a task on its own: the only
//! wakeups come from the IO transport. Pings and probes are started by
//! the caller ([`Transport::init_ping`], [`Transport::init_probe`]) and
//! are never given up on. Every transition is reported as a
//! [`ProbeEvent`], which is what lets a caller impose its own schedule
//! and its own deadlines - including the deadline that detects a dead
//! peer, since an unfinished probe pauses data output indefinitely.

use std::{
    fmt, io,
    num::NonZeroU32,
    ops::Not,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::Instant,
};

use crate::{
    buffer,
    codec::{
        RammuxCodec,
        decoder::{DecodedFrame, StreamPayload},
        encoder::EncoderItem,
    },
    config::{RammuxConfig, RammuxRole},
    error::{ErrorKind, RammuxError},
    probe::{Probe, ProbeDone, ProbeFrame},
    stream_id::StreamId,
};

pub use crate::{header::ControlFlags, probe::ProbeEvent};

/// A stream-level frame: the only traffic a [`Transport`] passes through.
///
/// Everything else on the wire - `PING`, `CLEAR_LINK`,
/// `SESSION_WINDOW_UPDATE`, `TERM` - is the transport's own business and
/// never reaches the caller as a frame.
#[derive(Debug, PartialEq)]
pub enum StreamFrame {
    /// Payload for one stream.
    Data(Data),
    /// Receive window credit for one stream.
    WindowUpdate(StreamWindowUpdate),
}

impl StreamFrame {
    /// ID of the stream this frame belongs to.
    pub fn stream_id(&self) -> StreamId {
        match self {
            Self::Data(frame) => frame.stream_id,
            Self::WindowUpdate(frame) => frame.stream_id,
        }
    }

    /// Stream lifecycle bits this frame carries.
    pub fn flags(&self) -> ControlFlags {
        match self {
            Self::Data(frame) => frame.flags,
            Self::WindowUpdate(frame) => frame.flags,
        }
    }
}

/// A `DATA` frame: payload for one stream.
#[derive(Debug, PartialEq)]
pub struct Data {
    /// Stream the payload belongs to.
    pub stream_id: StreamId,
    /// Stream lifecycle bits carried along.
    pub flags: ControlFlags,
    /// The payload itself. Counts against the transit window.
    pub payload: Payload,
}

/// A stream-level `WINDOW_UPDATE` frame.
///
/// Window updates cost no transit credit: they are what keeps the
/// *reverse* direction moving, so holding them back behind an exhausted
/// transit window would stall the peer on credit only we can return.
#[derive(Debug, PartialEq, Eq)]
pub struct StreamWindowUpdate {
    /// Stream the credit belongs to.
    pub stream_id: StreamId,
    /// Stream lifecycle bits carried along.
    pub flags: ControlFlags,
    /// Bytes of receive window credit returned. May be zero, which is how
    /// a bare `SYN` or `FIN_*` travels.
    pub update: u32,
}

/// Payload of a [`Data`] frame.
///
/// Not simply [`Bytes`], because the two directions get here differently
/// and both are free of copies: payload read off the transport lands in a
/// muxer-owned buffer that a reader queue can link into without a second
/// allocation, while payload handed over to be sent is already [`Bytes`].
#[derive(Clone, Default)]
pub struct Payload(Repr);

#[derive(Clone)]
enum Repr {
    /// Read off the transport into a muxer-owned buffer.
    Received(buffer::Data),
    /// Handed to the transport to send.
    ToSend(Bytes),
}

impl Default for Repr {
    fn default() -> Self {
        Self::ToSend(Bytes::new())
    }
}

impl Payload {
    /// Length of the payload in bytes.
    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// Whether the payload is empty. Empty `DATA` frames are valid, and
    /// are how a bare `FIN_WRITE` travels.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Takes the payload in the form a stream's reader queue accepts.
    ///
    /// The muxer only ever feeds this back what it read, so the copy is
    /// a path it does not take; accepting caller-supplied payload that
    /// way is still better than refusing it.
    pub(crate) fn into_received(self) -> buffer::Data {
        match self.0 {
            Repr::Received(data) => data,
            Repr::ToSend(bytes) => buffer::Data::copy_from_slice(&bytes),
        }
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Repr::Received(data) => data.as_ref(),
            Repr::ToSend(bytes) => bytes,
        }
    }
}

impl From<Bytes> for Payload {
    fn from(value: Bytes) -> Self {
        Self(Repr::ToSend(value))
    }
}

impl From<buffer::Data> for Payload {
    fn from(value: buffer::Data) -> Self {
        Self(Repr::Received(value))
    }
}

impl From<Payload> for Bytes {
    fn from(value: Payload) -> Self {
        match value.0 {
            Repr::Received(data) => data.into(),
            Repr::ToSend(bytes) => bytes,
        }
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Payload").field(&self.len()).finish()
    }
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

/// A round trip time sample, and when it was taken.
///
/// The timestamp is what makes a sample usable: rammux measures only when
/// asked to, so how old a measurement is depends entirely on the caller's
/// schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeasuredRtt {
    /// The measured round trip.
    pub value: Duration,
    /// When the sample completed.
    pub at: Instant,
}

/// Wrapper over [`AsyncRead`]/[`AsyncWrite`] that transparently handles
/// the transit window, the plain ping, and the link-clearing probe.
///
/// See the [module docs](self) for what that covers.
///
/// # Reading
///
/// The [`Stream`] side yields stream frames and is exhausted when the
/// peer's `TERM` arrives. Polling it also drives the transport's own
/// outbound traffic, so a peer that only ever reads still answers pings
/// and returns transit credit.
///
/// # Writing
///
/// The [`Sink`] side accepts stream frames. It is not ready while a probe
/// owns the link, and it never accepts more `DATA` payload than
/// [`Transport::send_budget`] allows - the caller must size its frames
/// against that budget. Once the peer's `TERM` has arrived, frames are
/// accepted and dropped: there is no longer anybody to read them.
///
/// [`Sink::poll_close`] sends this side's `TERM` and flushes it. It does
/// *not* shut the IO transport down, because the point of the handshake
/// is to hand that transport back usable - see [`Transport::into_inner`].
pub struct Transport<IO> {
    /// Framing over the IO transport.
    codec: RammuxCodec<IO>,
    /// The `PING` machinery: plain ping and link-clearing probe.
    probe: Probe,
    /// Credit the peer granted us, if the transit window is enabled.
    transit_send: Option<u32>,
    /// The transit window we grant the peer, if enabled.
    transit_recv: Option<TransitRecv>,
    /// Latest sample over a drained link.
    clean_rtt: Option<MeasuredRtt>,
    /// Latest sample with the connection's queues standing.
    dirty_rtt: Option<MeasuredRtt>,
    /// The peer's `TERM` has arrived.
    terminated: bool,
    /// Our `TERM` has been queued; nothing may follow it.
    term_sent: bool,
    /// A frame decoded while the [`Sink`] side was driving the reader to
    /// get itself unblocked, waiting for the [`Stream`] side to take it.
    held: Option<StreamFrame>,
    /// Something happened that may unblock a held-back sender.
    /// Drained by [`Transport::take_unblocked`].
    unblocked: bool,
    /// When the current transit stall began, if the sender is stalled.
    stalled_since: Option<Instant>,
    /// Total time the sender spent stalled on transit credit.
    stalled_total: Duration,
    /// How many transit stalls the sender has entered.
    stalled_events: u64,
}

impl<IO> Transport<IO> {
    /// Creates a new transport over `io`.
    ///
    /// The initial state is the opening `PING` exchange: neither side has
    /// sent anything yet, so the link is clear by construction and the
    /// first sample needs no `CLEAR_LINK` - both sides simply `PING` as
    /// their very first frame, one round trip instead of the full
    /// clearing dance. Stream frames are held back until it completes,
    /// and it fills in both [`Transport::clean_rtt`] and
    /// [`Transport::dirty_rtt`]: on an empty link the two are the same
    /// measurement.
    pub fn new(role: RammuxRole, io: IO, config: &RammuxConfig) -> Self {
        Self {
            codec: RammuxCodec::new(io, config.frame_limit),
            probe: Probe::new(role),
            transit_send: (config.remote_transit_window > 0)
                .then_some(config.remote_transit_window),
            transit_recv: NonZeroU32::new(config.local_transit_window)
                .map(|initial| TransitRecv::new(initial, config.transit_window_max)),
            clean_rtt: None,
            dirty_rtt: None,
            terminated: false,
            term_sent: false,
            held: None,
            unblocked: false,
            stalled_since: None,
            stalled_total: Duration::ZERO,
            stalled_events: 0,
        }
    }

    /// Initiates a ping-pong exchange, if currently possible.
    ///
    /// The ping travels inline with data, so it times the round trip
    /// through the queues that are actually standing - the loaded RTT,
    /// reported by [`Transport::dirty_rtt`].
    ///
    /// Returns whether the exchange was initiated. It is refused if:
    /// 1. an exchange initiated by this side is already in progress,
    /// 2. a probe is in progress - a `PING` there belongs to the dance,
    /// 3. the peer terminated the rammux connection.
    pub fn init_ping(&mut self) -> bool {
        self.terminated.not() && self.probe.send_ping()
    }

    /// Initiates a link-clearing probe, if currently possible.
    ///
    /// Both sides pause data output, drain the link with a `CLEAR_LINK`
    /// exchange, and time a `PING` over it - the clean RTT, reported by
    /// [`Transport::clean_rtt`]. The transit window autotunes against it,
    /// so a connection that never probes never grows its window.
    ///
    /// Returns whether the probe was initiated. It is refused if:
    /// 2. a probe is already in progress - including one the peer
    ///    initiated, and the opening exchange,
    /// 3. the peer terminated the rammux connection.
    ///
    /// A ping-pong exchange in flight does not refuse it: the probe takes
    /// the link and gives that ping up, since its pong would otherwise be
    /// indistinguishable from the probe's own. Whatever was given up is
    /// reported as [`ProbeEvent::PingAbandoned`].
    pub fn init_probe(&mut self) -> bool {
        self.terminated.not() && self.probe.start()
    }

    /// Gives up on the outstanding plain `PING`, so [`Transport::init_ping`]
    /// can run again.
    ///
    /// Returns whether one was in flight. A `PONG` that arrives for it
    /// afterwards is ignored rather than failing the connection. This is
    /// how a caller enforces a deadline on a ping: nothing here gives up
    /// on one by itself.
    pub fn abandon_ping(&mut self) -> bool {
        self.probe.give_up_ping()
    }

    /// Returns the latest RTT measured on a clear link.
    pub fn clean_rtt(&self) -> Option<MeasuredRtt> {
        self.clean_rtt
    }

    /// Returns the latest RTT measured on a dirty link (with data
    /// possibly flowing).
    pub fn dirty_rtt(&self) -> Option<MeasuredRtt> {
        self.dirty_rtt
    }

    /// Takes the next transition of the RTT machinery, if any.
    ///
    /// See [`ProbeEvent`] for what to do with them.
    pub fn next_event(&mut self) -> Option<ProbeEvent> {
        self.probe.next_event()
    }

    /// How much more `DATA` payload the transit window allows in flight,
    /// or [`None`] if the transit window is disabled in this direction.
    ///
    /// A [`Data`] frame handed to [`Sink::start_send`] must not carry more
    /// than this.
    pub fn send_budget(&self) -> Option<u32> {
        self.transit_send
    }

    /// Current size of the transit window we grant the peer, if enabled.
    pub fn transit_window(&self) -> Option<u32> {
        self.transit_recv.as_ref().map(|recv| recv.current)
    }

    /// Whether the peer has terminated the rammux connection.
    pub fn terminated(&self) -> bool {
        self.terminated
    }

    /// Whether something has happened that may unblock a sender this
    /// transport was holding back: a probe finished, or transit credit
    /// arrived. Both leave senders parked on wakers only the caller has,
    /// so the caller has to wake them.
    ///
    /// Consumes the signal.
    pub fn take_unblocked(&mut self) -> bool {
        std::mem::take(&mut self.unblocked)
    }

    /// Total time the sender spent stalled on a transit credit grant, and
    /// how many such stalls it entered.
    ///
    /// A stall is time in which the transport was writable and stream
    /// payload was ready, but the transit window was spent. Time with a
    /// merely *fully utilised* window is not a stall: the link is busy
    /// draining it.
    pub fn transit_stalls(&self) -> (Duration, u64) {
        let pending = self
            .stalled_since
            .map(|at| at.elapsed())
            .unwrap_or_default();
        (self.stalled_total + pending, self.stalled_events)
    }

    /// Accounts for one completed pass of the caller's send loop.
    ///
    /// `blocked` means the transport was ready to accept a frame and the
    /// caller had payload for it, but [`Transport::send_budget`] was
    /// spent. That is the only state in which the credit-return loop -
    /// not the link, the peer's stream windows, or the probe pause - is
    /// what holds the sender back.
    pub fn note_send_pass(&mut self, blocked: bool) {
        match (blocked, self.stalled_since) {
            (true, None) => {
                self.stalled_since = Some(Instant::now());
                self.stalled_events += 1;
            },
            (false, Some(at)) => {
                self.stalled_total += at.elapsed();
                self.stalled_since = None;
            },
            _ => {},
        }
    }

    /// Consumes this wrapper, returning the raw IO and the already read
    /// data.
    ///
    /// The bytes are whatever was read off the transport but not yet
    /// decoded into a frame. After the `TERM` handshake there are none:
    /// the decoder reads exactly one frame at a time, so it never runs
    /// past the last one.
    pub fn into_inner(self) -> (IO, Bytes) {
        self.codec.into_parts()
    }

    /// Notes `len` bytes of `DATA` payload received and stored, and
    /// tracks the arrival rate the transit window autotunes against.
    fn transit_recv_freed(&mut self, len: usize) {
        let rtt = self.clean_rtt.map(|rtt| rtt.value);
        let Some(recv) = self.transit_recv.as_mut() else {
            return;
        };
        let len = u32::try_from(len).unwrap_or(u32::MAX);
        recv.freed = recv.freed.saturating_add(len);
        // Measure the arrival rate over buckets of at least
        // max(2 x RTT, 10ms) - rates sampled over shorter horizons are
        // burst artifacts, not throughput.
        recv.rate_bucket_bytes += u64::from(len);
        let bucket = (2 * rtt.unwrap_or(Duration::from_millis(5))).max(Duration::from_millis(10));
        let elapsed = recv.rate_bucket_start.elapsed();
        if elapsed >= bucket {
            let rate = recv.rate_bucket_bytes as f64 / elapsed.as_secs_f64();
            recv.rate_ema = if recv.rate_ema == 0.0 {
                rate
            } else {
                0.5 * recv.rate_ema + 0.5 * rate
            };
            recv.rate_bucket_bytes = 0;
            recv.rate_bucket_start = Instant::now();
        }
    }

    /// Produces the next `SESSION_WINDOW_UPDATE` value, if one is due.
    fn transit_recv_update(&mut self) -> Option<u32> {
        let clean_rtt = self.clean_rtt.map(|rtt| rtt.value);
        let recv = self.transit_recv.as_mut()?;
        if recv.can_update().not() {
            return None;
        }

        // Growth needs evidence of window limitation, judged against the
        // clean-probe RTT - a yardstick our own queue cannot inflate.
        // The measured arrival rate is capped by the current window, so
        // it cannot size the next window directly (chicken-and-egg);
        // instead it bounds growth: the window doubles while limited,
        // ceilinged at 2 x rate x clean RTT, which stops growth at
        // about twice the loop BDP once the link is the bottleneck.
        // Grow-only.
        //
        // The ceiling stays a multiple of the *clean* BDP. Sizing it from
        // the loaded RTT runs away: a bigger window queues more, which
        // raises the loaded RTT, which raises the ceiling again.
        let elapsed = recv.last_update.elapsed();
        let optimal = match clean_rtt {
            Some(rtt) if recv.rate_ema > 0.0 && elapsed < 2 * rtt => {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let ceiling = (2.0 * rtt.as_secs_f64() * recv.rate_ema) as u32;
                recv.current
                    .saturating_mul(2)
                    .min(ceiling)
                    .max(recv.current)
            },
            _ => recv.current,
        };
        let clamped = optimal.min(recv.max);

        let update = recv.freed + (clamped - recv.current);
        recv.current = clamped;
        recv.freed = 0;
        recv.last_update = Instant::now();

        (update > 0).then_some(update)
    }
}

impl<IO> Transport<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Queues everything the transport itself owes the peer: probe frames
    /// first, then transit credit.
    ///
    /// Nothing follows our own `TERM`, so this stops once that is queued.
    fn poll_own_frames(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        if self.term_sent {
            return Poll::Ready(Ok(()));
        }
        loop {
            std::task::ready!(self.codec.poll_ready_unpin(cx))?;

            if let Some((frame, resume)) = self.probe.next_frame() {
                let item = match frame {
                    ProbeFrame::Clear(syn) => EncoderItem::new_clear_link(syn),
                    ProbeFrame::Ping(payload) => EncoderItem::new_ping(payload, false),
                    ProbeFrame::Pong(payload) => EncoderItem::new_ping(payload, true),
                };
                self.codec.start_send_unpin(item)?;
                self.unblocked |= resume;
                continue;
            }

            // Credit is only worth returning while the peer can still use
            // it, and a probe pause must not be broken by a session frame.
            if self.terminated || self.probe.paused() {
                return Poll::Ready(Ok(()));
            }
            match self.transit_recv_update() {
                Some(update) => {
                    self.codec
                        .start_send_unpin(EncoderItem::new_session_window_update(update))?;
                },
                None => return Poll::Ready(Ok(())),
            }
        }
    }

    /// Reads until a stream frame arrives, the peer terminates, or the
    /// transport blocks. Everything that is the transport's own business
    /// is consumed here rather than surfaced.
    fn poll_inbound(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<StreamFrame, ErrorKind>>> {
        loop {
            let frame = match std::task::ready!(self.codec.poll_next_unpin(cx)) {
                Some(Ok(frame)) => frame,
                Some(Err(error)) => return Poll::Ready(Some(Err(error))),
                None => {
                    return Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())));
                },
            };

            let stream_frame = match frame {
                DecodedFrame::Terminate => {
                    self.terminated = true;
                    // Whatever the sender was waiting for, it is moot now.
                    self.unblocked = true;
                    return Poll::Ready(None);
                },

                DecodedFrame::ClearLink { syn } => {
                    self.probe.on_clear_link(syn)?;
                    None
                },

                DecodedFrame::Ping {
                    payload,
                    is_response,
                } => {
                    let done = if is_response {
                        self.probe.on_pong(payload)?
                    } else {
                        self.probe.on_ping(payload)?
                    };
                    // Loaded samples come from the plain ping and from the
                    // probe's own CLEAR_LINK leg, so they are drained here
                    // rather than carried on probe completion.
                    if let Some(value) = self.probe.take_dirty_rtt() {
                        self.dirty_rtt = Some(MeasuredRtt {
                            value,
                            at: Instant::now(),
                        });
                    }
                    if let Some(ProbeDone { rtt, resume }) = done {
                        let at = Instant::now();
                        self.clean_rtt = Some(MeasuredRtt { value: rtt, at });
                        // The opening exchange runs on a link that is
                        // clear by construction, so its sample is both.
                        self.dirty_rtt.get_or_insert(MeasuredRtt { value: rtt, at });
                        self.unblocked |= resume;
                    }
                    None
                },

                // Past the peer's CLEAR_LINK its direction is drained:
                // only probe frames may arrive until the probe runs its
                // course on the peer's side.
                DecodedFrame::Stream { .. } | DecodedFrame::SessionWindowUpdate { .. }
                    if self.probe.peer_must_be_silent() =>
                {
                    return Poll::Ready(Some(Err(ErrorKind::Probe(
                        "received a non-probe frame while the link is being cleared",
                    ))));
                },

                DecodedFrame::SessionWindowUpdate { update } => {
                    let credit = self
                        .transit_send
                        .as_mut()
                        .ok_or(ErrorKind::Transit("transit window is not enabled"))?;
                    *credit = credit
                        .checked_add(update)
                        .ok_or(ErrorKind::Transit("transit window overflow"))?;
                    if update > 0 {
                        // The grant is what the sender was stalled on, so
                        // the stall ends here rather than on its next pass.
                        self.note_send_pass(false);
                        self.unblocked = true;
                    }
                    None
                },

                DecodedFrame::Stream {
                    stream_id,
                    flags,
                    payload,
                } => Some(match payload {
                    StreamPayload::WindowUpdate(update) => {
                        StreamFrame::WindowUpdate(StreamWindowUpdate {
                            stream_id,
                            flags,
                            update,
                        })
                    },
                    StreamPayload::Data(data) => {
                        // Transit credit is freed as soon as the payload
                        // is stored here: the bytes have left the link,
                        // which is all this window accounts for.
                        self.transit_recv_freed(data.as_ref().len());
                        StreamFrame::Data(Data {
                            stream_id,
                            flags,
                            payload: data.into(),
                        })
                    },
                }),
            };

            // Answering what just arrived is the transport's own job, and
            // it must not wait for the caller to write something.
            if let Poll::Ready(Err(error)) = self.poll_own_frames(cx) {
                return Poll::Ready(Some(Err(error)));
            }
            if let Some(frame) = stream_frame {
                return Poll::Ready(Some(Ok(frame)));
            }
        }
    }
}

impl<IO> Transport<IO>
where
    IO: AsyncWrite + Unpin,
{
    /// Flushes and shuts the IO transport down.
    ///
    /// [`Sink::poll_close`] deliberately does not: a downgrade hands the
    /// transport back usable. This is for a caller that is finished with
    /// it anyway, and saves the round trips a later shutdown would cost.
    pub fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RammuxError>> {
        self.codec.poll_close_unpin(cx).map_err(From::from)
    }
}

impl<IO> Stream for Transport<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<StreamFrame, RammuxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(frame) = this.held.take() {
            return Poll::Ready(Some(Ok(frame)));
        }
        if this.terminated {
            return Poll::Ready(None);
        }
        // Keep the transport's own traffic moving even for a caller that
        // only ever reads: pongs and transit credit are owed regardless.
        if let Poll::Ready(Err(error)) = this.poll_own_frames(cx) {
            return Poll::Ready(Some(Err(error.into())));
        }
        this.poll_inbound(cx)
            .map(|frame| frame.map(|frame| frame.map_err(From::from)))
    }
}

impl<IO> Sink<StreamFrame> for Transport<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    type Error = RammuxError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        loop {
            std::task::ready!(this.poll_own_frames(cx))?;
            if this.terminated || this.probe.paused().not() {
                return this.codec.poll_ready_unpin(cx).map_err(From::from);
            }
            if this.held.is_some() {
                // The caller has a frame waiting on the Stream side, and
                // taking it is the only thing that gets us further. The
                // read that produced it registered no waker - it returned
                // ready - so wake ourselves rather than leave a caller
                // that only drives the Sink parked forever.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            // Only an inbound frame ends a probe, so read to make
            // progress rather than parking on a waker nothing will fire.
            match std::task::ready!(this.poll_inbound(cx)) {
                Some(Ok(frame)) => this.held = Some(frame),
                Some(Err(error)) => return Poll::Ready(Err(error.into())),
                None => {},
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: StreamFrame) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if this.terminated {
            // Nobody is going to read it.
            return Ok(());
        }
        let item = match item {
            StreamFrame::WindowUpdate(frame) => {
                EncoderItem::new_window_update(frame.stream_id, frame.flags, frame.update)
            },
            StreamFrame::Data(frame) => {
                let len = u32::try_from(frame.payload.len()).unwrap_or(u32::MAX);
                if let Some(credit) = this.transit_send.as_mut() {
                    *credit = credit.checked_sub(len).ok_or(ErrorKind::Transit(
                        "sent more data than the transit window allows",
                    ))?;
                }
                if len > 0 {
                    // Payload moved, so whatever the sender was waiting
                    // for, it was not a credit grant.
                    this.note_send_pass(false);
                }
                EncoderItem::new_data(frame.stream_id, frame.flags, frame.payload.into())
            },
        };
        this.codec.start_send_unpin(item).map_err(From::from)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Poll::Ready(Err(error)) = this.poll_own_frames(cx) {
            return Poll::Ready(Err(error.into()));
        }
        this.codec.poll_flush_unpin(cx).map_err(From::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.term_sent.not() {
            std::task::ready!(this.poll_own_frames(cx))?;
            std::task::ready!(this.codec.poll_ready_unpin(cx))?;
            this.codec.start_send_unpin(EncoderItem::new_terminate())?;
            this.term_sent = true;
        }
        this.codec.poll_flush_unpin(cx).map_err(From::from)
    }
}

/// The transit window we grant the peer: how much `DATA` payload it may
/// have in flight towards us, and the credit owed back to it.
///
/// Credit is freed as soon as payload is received and stored, independently
/// of when the application reads it.
struct TransitRecv {
    /// Current window size.
    current: u32,
    /// Bytes freed since the last update.
    freed: u32,
    /// Autotune growth limit.
    max: u32,
    /// Smoothed arrival rate, bytes per second.
    rate_ema: f64,
    /// Start of the current rate-measurement bucket.
    rate_bucket_start: Instant,
    /// Payload bytes received in the current rate-measurement bucket.
    rate_bucket_bytes: u64,
    /// Time of the last produced window update.
    last_update: Instant,
}

impl TransitRecv {
    fn new(initial: NonZeroU32, max: u32) -> Self {
        Self {
            current: initial.get(),
            freed: 0,
            max: max.max(initial.get()),
            rate_ema: 0.0,
            rate_bucket_start: Instant::now(),
            rate_bucket_bytes: 0,
            last_update: Instant::now(),
        }
    }

    /// Whether a `SESSION_WINDOW_UPDATE` is due.
    fn can_update(&self) -> bool {
        self.freed >= self.current / 2
    }
}

#[cfg(test)]
mod test {
    use futures::{FutureExt, future::poll_fn};
    use tokio::io::DuplexStream;

    use super::*;

    /// Two transports over one in-memory link, plus what each has read.
    struct Link {
        client: Transport<DuplexStream>,
        server: Transport<DuplexStream>,
        /// Stream frames the server read.
        to_server: Vec<StreamFrame>,
        /// Stream frames the client read.
        to_client: Vec<StreamFrame>,
    }

    impl Link {
        fn new(config: &RammuxConfig) -> Self {
            let (io_1, io_2) = tokio::io::duplex(64 * 1024);
            Self {
                client: Transport::new(RammuxRole::Client, io_1, config),
                server: Transport::new(RammuxRole::Server, io_2, config),
                to_server: Vec::new(),
                to_client: Vec::new(),
            }
        }

        /// One pass over both sides: read what is ready, then flush.
        ///
        /// Polling a transport queues the frames it owes but does not put
        /// them on the wire, so a driver has to flush - which is what the
        /// connection above does on every pass too.
        async fn step(&mut self) {
            let Self {
                client,
                server,
                to_server,
                to_client,
            } = self;
            poll_fn(|cx| {
                while let Poll::Ready(Some(frame)) = client.poll_next_unpin(cx) {
                    to_client.push(frame.expect("client failed"));
                }
                while let Poll::Ready(Some(frame)) = server.poll_next_unpin(cx) {
                    to_server.push(frame.expect("server failed"));
                }
                let _ = client.poll_flush_unpin(cx);
                let _ = server.poll_flush_unpin(cx);
                Poll::Ready(())
            })
            .await;
            tokio::task::yield_now().await;
        }

        /// Runs both sides until `done`.
        async fn run(&mut self, mut done: impl FnMut(&Self) -> bool) {
            for _ in 0..64 {
                if done(self) {
                    return;
                }
                self.step().await;
            }
            panic!("the transports stalled");
        }

        /// Runs the opening exchange out of the way.
        async fn settle(&mut self) {
            self.run(|link| link.client.clean_rtt().is_some() && link.server.clean_rtt().is_some())
                .await;
            assert!(
                self.to_client.is_empty() && self.to_server.is_empty(),
                "the opening exchange never reaches the caller"
            );
        }

        /// Hands one frame to the client's sink, which must be ready.
        fn send(&mut self, frame: StreamFrame) -> Result<(), RammuxError> {
            poll_fn(|cx| self.client.poll_ready_unpin(cx))
                .now_or_never()
                .expect("the sink is not ready")?;
            self.client.start_send_unpin(frame)
        }
    }

    fn data(id: u8, payload: &'static [u8]) -> StreamFrame {
        StreamFrame::Data(Data {
            stream_id: StreamId::from_be_bytes([0, 0, id]),
            flags: ControlFlags::default(),
            payload: Bytes::from_static(payload).into(),
        })
    }

    fn window_update(id: u8, update: u32) -> StreamFrame {
        StreamFrame::WindowUpdate(StreamWindowUpdate {
            stream_id: StreamId::from_be_bytes([0, 0, id]),
            flags: ControlFlags::default(),
            update,
        })
    }

    /// The opening exchange never surfaces: a caller sees stream frames
    /// and an RTT that filled itself in.
    #[tokio::test]
    async fn the_opening_exchange_is_transparent() {
        let mut link = Link::new(&RammuxConfig::new());
        assert!(link.client.clean_rtt().is_none());
        link.settle().await;

        for side in [&link.client, &link.server] {
            assert!(side.clean_rtt().is_some());
            assert!(
                side.dirty_rtt().is_some(),
                "on an empty link the clean sample is also the loaded one"
            );
        }
    }

    #[tokio::test]
    async fn stream_frames_round_trip() {
        let mut link = Link::new(&RammuxConfig::new());
        link.settle().await;

        link.send(data(2, b"payload")).unwrap();
        link.send(window_update(2, 4096)).unwrap();
        link.run(|link| link.to_server.len() == 2).await;
        assert_eq!(
            link.to_server,
            [data(2, b"payload"), window_update(2, 4096)]
        );
        assert!(link.to_client.is_empty());
    }

    /// The transit window paces payload and refills itself: the peer's
    /// `SESSION_WINDOW_UPDATE`s are consumed here, never surfaced.
    #[tokio::test]
    async fn the_transit_window_paces_and_refills() {
        let mut config = RammuxConfig::new();
        config.local_transit_window = 64;
        config.remote_transit_window = 64;
        config.transit_window_max = 64;
        let mut link = Link::new(&config);
        link.settle().await;

        assert_eq!(link.client.send_budget(), Some(64));
        link.send(data(2, &[b'x'; 48])).unwrap();
        assert_eq!(link.client.send_budget(), Some(16), "payload cost credit");
        // Window updates are free: they are what keeps the peer sending,
        // so an exhausted transit window must not hold them back.
        link.send(window_update(2, 1024)).unwrap();
        assert_eq!(link.client.send_budget(), Some(16));

        // The peer frees the credit as soon as it reads the payload, and
        // returning it is the transport's own business.
        link.run(|link| link.client.send_budget() == Some(64)).await;
        assert_eq!(
            link.to_server,
            [data(2, &[b'x'; 48]), window_update(2, 1024)]
        );
        assert!(
            link.client.take_unblocked(),
            "the grant has to wake whoever was waiting on it"
        );
    }

    /// Overspending the transit window is a protocol violation, not a
    /// silent overrun: the caller has to respect the budget.
    #[tokio::test]
    async fn overspending_the_transit_window_fails() {
        let mut config = RammuxConfig::new();
        config.local_transit_window = 16;
        config.remote_transit_window = 16;
        let mut link = Link::new(&config);
        link.settle().await;

        let error = link.send(data(2, &[b'x'; 32])).unwrap_err();
        assert!(error.is_protocol_violation());
    }

    /// A probe holds stream frames back until it completes.
    #[tokio::test]
    async fn a_probe_holds_the_sink() {
        let mut link = Link::new(&RammuxConfig::new());
        link.settle().await;

        assert!(link.client.init_probe());
        assert!(
            poll_fn(|cx| link.client.poll_ready_unpin(cx))
                .now_or_never()
                .is_none(),
            "the probe owns the link"
        );
    }

    /// The dance runs itself through once both sides are polled, and the
    /// sink opens up again with nothing of it visible to either caller.
    #[tokio::test]
    async fn a_probe_completes_on_its_own() {
        let mut link = Link::new(&RammuxConfig::new());
        link.settle().await;
        let before = link.client.clean_rtt().expect("opening sample");

        assert!(link.client.init_probe());
        assert!(!link.client.init_probe(), "one probe at a time");
        link.run(|link| link.client.clean_rtt() != Some(before))
            .await;

        link.send(data(2, b"after")).unwrap();
        link.run(|link| !link.to_server.is_empty()).await;
        assert_eq!(link.to_server, [data(2, b"after")]);
        assert!(link.to_client.is_empty(), "the dance stayed out of sight");
    }

    /// `TERM` exhausts the stream, and what the caller writes afterwards
    /// is dropped rather than failing: nobody is left to read it.
    #[tokio::test]
    async fn term_exhausts_the_stream() {
        let mut link = Link::new(&RammuxConfig::new());
        link.settle().await;

        poll_fn(|cx| link.client.poll_close_unpin(cx))
            .await
            .unwrap();
        link.run(|link| link.server.terminated()).await;

        poll_fn(|cx| link.server.poll_ready_unpin(cx))
            .await
            .unwrap();
        link.server
            .start_send_unpin(data(3, b"into the void"))
            .unwrap();
        assert!(!link.server.init_ping());
        assert!(!link.server.init_probe());

        let (_io, unread) = link.server.into_inner();
        assert!(unread.is_empty(), "the decoder never reads past a frame");
    }
}
