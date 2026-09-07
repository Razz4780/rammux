//! The connection itself: framing, the sender's credit, and the poll loop.
//!
//! The mental model this serves is at the [crate root](crate); the decisions it
//! carries out live in [`crate::window`], [`crate::delay`] and [`crate::probe`].
//! What is here is the shell that wires them to an
//! [`AsyncRead`] + [`AsyncWrite`] transport.
//!
//! # Two socket settings a caller has to get right
//!
//! Neither is this module's to set - the transport arrives already configured -
//! but both silently invalidate the window if they are wrong.
//!
//! * **`TCP_NODELAY` on both ends.** Credit returns are 8 byte frames on the
//!   latency path, and Nagle would hold every one of them behind the data
//!   already in flight.
//! * **`SO_SNDBUF` above the window.** It is a second, hidden limiter: smaller
//!   than `W` it binds instead, and the connection then measures the socket
//!   buffer rather than the protocol. Linux autotunes it toward twice the
//!   congestion window, bounded by `net.ipv4.tcp_wmem`, so the ceiling is what
//!   needs to clear [`Sizing::max`] - not the value at connect time.
//!
//! # Wakers, and the deadlock that taught us to care
//!
//! Every path that returns `Pending` must leave a waker registered with
//! something that will fire. That is easy to get wrong here, because a
//! connection has two reasons to wait - no credit, and a full socket - and only
//! one of them is the socket's business.
//!
//! The rule that keeps it honest: **never stop reading the transport**. An
//! earlier version bounded memory by refusing to read once undelivered payload
//! reached the window, which also refused the `WINDOW_UPDATE` that would have
//! unblocked it, and left `poll_write` parked with no waker anywhere. Memory is
//! bounded by withholding the *grant* instead.
//!
//! # Half-close
//!
//! Control frames share the write direction with payload, so an end that has
//! shut its write side down can neither return credit nor take part in a
//! probe. Shutting down therefore ends this end's part in the protocol, not
//! just its payload: nothing more goes on the wire, a probe is never started,
//! and a peer that keeps sending can only spend the credit it already holds -
//! which is what half-close means with a credit protocol underneath. The peer,
//! on seeing the end of stream, drops out of any exchange it was in rather
//! than waiting the deadline out for a `PING` that cannot come, and starts no
//! more of its own. Reading past a local shutdown works as it does on a
//! socket.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Instant,
};

use crate::{
    frame::{Frame, HEADER_LEN, MAX_PAYLOAD, TIMESTAMP_LEN},
    probe::Probe,
    window::{Sizing, Window},
};

pub use crate::probe::Role;

/// How many of the last probe's durations to wait before the next one.
///
/// Measured across four links: this settles the schedule at 6 to 16 seconds and
/// matches a hand-tuned fixed interval's throughput on every one of them. See
/// [`Config::probe_spacing`].
pub const DEFAULT_PROBE_SPACING: f64 = 128.0;

/// How much encoded output may queue here before writes go pending.
///
/// The window already bounds what may sit in this buffer; the cap only keeps a
/// socket that has stopped taking bytes from letting it fill with a whole
/// window in one go.
const OUT_HIGH_WATER: usize = 2 * MAX_PAYLOAD as usize;

/// Everything a connection needs to be configured with.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// The bounds the window is sized within, and the rule that sizes it.
    pub sizing: Sizing,
    /// How many of the last probe's durations to wait before the next one.
    ///
    /// An exchange's duration is its cost, so spacing them by their own
    /// duration bounds the share of a connection's time spent probing on any
    /// link at once, where a fixed interval could only be tuned to some.
    pub probe_spacing: f64,
    /// Which end schedules the probe. Exactly one end must be the
    /// [`Role::Initiator`]; see [`crate::probe`].
    pub role: Role,
}

impl Config {
    /// The tuned configuration, for a connection in this role.
    ///
    /// Every field is public, so this is also the starting point for a
    /// modified one:
    ///
    /// ```
    /// use transit::{Config, Role};
    ///
    /// let mut config = Config::new(Role::Responder);
    /// config.sizing.max = 4 * 1024 * 1024;
    /// ```
    pub fn new(role: Role) -> Self {
        Self {
            sizing: Sizing::default(),
            probe_spacing: DEFAULT_PROBE_SPACING,
            role,
        }
    }

    /// Whether anything here uses the clean round trip, and so justifies the
    /// paused output a probe costs. Kept as a function so the answer has one
    /// place to change if a rule that does not need it ever comes back.
    fn wants_rtt(&self) -> bool {
        // The delay rule always does: its target is a fraction of the round
        // trip, its control interval is one, and the probe's drain is what
        // keeps its delay base honest. Pacing needs it as well.
        true
    }
}

impl From<Role> for Config {
    fn from(role: Role) -> Self {
        Self {
            sizing: Default::default(),
            probe_spacing: 128.0,
            role,
        }
    }
}

/// What a connection will say about itself, for logging and diagnosis.
///
/// Read with [`Transit::stats`]. Non-exhaustive: a connection may learn to
/// report more about itself without that being a breaking change.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Stats {
    /// The window we currently grant the peer.
    pub window: u32,
    /// What is left of the window the peer granted us.
    pub credit: u64,
    /// Smoothed arrival rate, bytes per second.
    pub rate: f64,
    /// The last round trip measured over a drained link, once one has been.
    pub clean_rtt: Option<Duration>,
    /// Probe exchanges completed.
    pub probes: u64,
    /// Probe exchanges given up on. Anything but zero says the peer is not
    /// answering, and the round trip being sized from is going stale.
    pub probe_timeouts: u64,
    /// The gap the probe schedule chose after its last exchange.
    pub probe_interval: Duration,
    /// How many times the sender ran out of credit with data to write.
    ///
    /// One per credit loop, so `stalls x re_grant / duration` is roughly the
    /// goodput whenever the window is what limits the sender. The wall time
    /// spent this way is deliberately not reported: an application that always
    /// has more to write hands its whole window to the socket at once and then
    /// waits, whatever the window is, so the figure approaches the whole run
    /// and says nothing.
    pub stalls: u64,
    /// Queuing delay the delay rule last acted on, if it is the rule in use.
    pub queued: Option<Duration>,
    /// The size that rule is steering towards.
    pub desired: u32,
}

/// How long a stall stays worth reporting.
///
/// The same figure libutp uses for the same guard (`last_maxed_out_window`):
/// a sender that has not hit its window in the last second is not window
/// limited now, whatever it was earlier, and telling the peer otherwise buys a
/// growth step nobody will use.
const STALL_REPORT_WINDOW: Duration = Duration::from_secs(1);

/// The sender's half: credit the peer granted, and whether we ran out of it.
#[derive(Debug)]
struct Credit {
    /// Payload bytes we may still put in flight.
    available: u64,
    /// Whether we are out of credit right now, so one stall is counted once
    /// however many times the application retries.
    starved: bool,
    /// When the stall the peer has not been told about yet began, if any.
    ///
    /// Separate from `starved` because the window update that ends a stall
    /// arrives *before* the next frame is written. Clearing on the update alone
    /// meant no frame ever carried the flag, and a receiver gating growth on it
    /// shrank to the floor.
    unreported: Option<Instant>,
    stalls: u64,
}

/// Sender timestamps back into a monotonic clock.
///
/// They are 32 bits of microseconds on the wire and wrap every 71 minutes.
/// libutp does all of its delay arithmetic in wrapping `uint32` for this
/// reason; unwrapping at the receiver is the equivalent for arithmetic done in
/// `i64`. Frames arrive in order and the sender's clock is monotonic, so a
/// timestamp below the previous one can only be a wrap. Without this, every
/// sample after a wrap reads 2^32 µs above the base, the window collapses to
/// the floor, and stays there for as long as the base's memory.
#[derive(Debug, Default)]
struct SenderClock {
    last: u32,
    laps: i64,
}

impl SenderClock {
    fn unwrap(&mut self, stamp: u32) -> i64 {
        if stamp < self.last {
            self.laps += 1;
        }
        self.last = stamp;
        i64::from(stamp) + (self.laps << 32)
    }
}

/// Where the inbound parser is.
#[derive(Debug, Clone, Copy)]
enum Parse {
    Header,
    /// After a `DATA` header, waiting for the sender timestamp that follows it.
    /// Carries the payload length the timestamp belongs to.
    Timestamp(u32),
    /// Inside a `DATA` frame with this many payload bytes to go.
    Payload(u32),
}

/// A transit window over `IO`.
///
/// Reads and writes payload transparently: the framing, the credit accounting
/// and the probe are not visible through [`AsyncRead`] and [`AsyncWrite`].
pub struct Transit<IO> {
    io: IO,

    /// Encoded frames waiting for the socket.
    out: BytesMut,
    credit: Credit,

    /// Read from the socket, not yet parsed.
    in_raw: BytesMut,
    /// Parsed payload waiting for the application.
    in_payload: BytesMut,
    parse: Parse,
    eof: bool,

    window: Window,
    probe: Probe,
    /// Whether our write side is shut down. Nothing goes on the wire after
    /// that; see the module docs on half-close.
    shut_down: bool,

    /// Our clock's origin, which every timestamp we send counts from.
    started: Instant,
    peer_clock: SenderClock,
}

impl<IO> Transit<IO> {
    /// Wraps `io`, granting the peer the initial window.
    ///
    /// The window is announced rather than assumed, so the two ends may be
    /// configured independently - only [`Config::role`] has to agree.
    ///
    /// Call this from inside a Tokio runtime: the probe schedule arms a timer
    /// here, and constructing one outside a runtime context panics.
    pub fn new(io: IO, config: Config) -> Self {
        let mut out = BytesMut::new();
        Frame::WindowUpdate(config.sizing.initial).encode(&mut out);
        Self {
            io,
            out,
            credit: Credit {
                available: 0,
                starved: false,
                unreported: None,
                stalls: 0,
            },
            in_raw: BytesMut::new(),
            in_payload: BytesMut::new(),
            parse: Parse::Header,
            eof: false,
            window: Window::new(config.sizing),
            probe: Probe::new(config.role, config.probe_spacing, config.wants_rtt()),
            shut_down: false,
            started: Instant::now(),
            peer_clock: SenderClock::default(),
        }
    }

    /// The transport underneath.
    ///
    /// Borrowed, not returned: settings can be read back off it - what
    /// `SO_SNDBUF` autotuned to, say - but taking it back would strand the
    /// credit and framing state that belong with it.
    pub fn get_ref(&self) -> &IO {
        &self.io
    }

    /// What the connection can say about itself right now.
    pub fn stats(&self) -> Stats {
        Stats {
            window: self.window.granted(),
            credit: self.credit.available,
            rate: self.window.rate(),
            clean_rtt: self.probe.clean_rtt(),
            probes: self.probe.completed(),
            probe_timeouts: self.probe.abandoned(),
            probe_interval: self.probe.interval(),
            stalls: self.credit.stalls,
            queued: self.window.queued(),
            desired: self.window.desired(),
        }
    }

    /// Handles a frame that is not `DATA`.
    fn on_control(&mut self, frame: Frame) -> io::Result<()> {
        match frame {
            Frame::Data { .. } => unreachable!("payload frames are handled by the parser"),
            Frame::WindowUpdate(granted) => {
                self.credit.available += u64::from(granted);
                self.credit.starved = false;
            },
            probe_frame => {
                let outcome = self.probe.on_frame(probe_frame)?;
                if let Some(reply) = outcome.reply {
                    reply.encode(&mut self.out);
                }
                if outcome.finished {
                    self.window.on_probe_end();
                }
            },
        }
        Ok(())
    }

    /// Folds an arriving frame's one-way delay into the queue estimate.
    ///
    /// The offset between the two clocks is unknown and lands in every sample
    /// identically, so it cancels when [`crate::delay`] subtracts its base.
    /// Nothing here needs the clocks to agree.
    fn on_timestamp(&mut self, sent: u32) {
        let sent = self.peer_clock.unwrap(sent);
        let arrived = self.started.elapsed().as_micros() as i64;
        self.window.on_delay(arrived - sent);
    }
}

impl<IO> Transit<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Pushes encoded frames at the socket until it stops taking them.
    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.out.is_empty() {
            match Pin::new(&mut self.io).poll_write(cx, &self.out) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(written)) => self.out.advance(written),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Turns whatever is in `in_raw` into payload and control handling.
    fn parse(&mut self) -> io::Result<()> {
        loop {
            match self.parse {
                Parse::Header => {
                    if self.in_raw.len() < HEADER_LEN {
                        return Ok(());
                    }
                    let frame = Frame::decode(&self.in_raw[..HEADER_LEN])?;
                    self.in_raw.advance(HEADER_LEN);
                    match frame {
                        Frame::Data { len, starved } => {
                            if starved {
                                self.window.note_peer_starved();
                            }
                            self.parse = Parse::Timestamp(len);
                        },
                        control => self.on_control(control)?,
                    }
                },
                Parse::Timestamp(len) => {
                    if self.in_raw.len() < TIMESTAMP_LEN {
                        return Ok(());
                    }
                    let sent = u32::from_be_bytes(self.in_raw[..TIMESTAMP_LEN].try_into().unwrap());
                    self.in_raw.advance(TIMESTAMP_LEN);
                    self.on_timestamp(sent);
                    self.parse = Parse::Payload(len);
                },
                // A zero-length frame is legal - it is a timestamp with nothing
                // attached - and has to move on without waiting for bytes that
                // are not coming.
                Parse::Payload(0) => self.parse = Parse::Header,
                Parse::Payload(remaining) => {
                    if self.in_raw.is_empty() {
                        return Ok(());
                    }
                    let take = (remaining as usize).min(self.in_raw.len());
                    self.in_payload.extend_from_slice(&self.in_raw[..take]);
                    self.in_raw.advance(take);
                    self.window.on_payload(take as u32, self.probe.clean_rtt());
                    // The window is the only thing bounding memory on this
                    // side, and a peer that ignores it is not applying
                    // backpressure to itself either. Better a clear error than
                    // an allocation that grows until something else fails.
                    if self.window.overdrawn() {
                        return Err(io::Error::other(
                            "peer sent more payload than it was granted credit for",
                        ));
                    }
                    let left = remaining - take as u32;
                    self.parse = if left == 0 {
                        Parse::Header
                    } else {
                        Parse::Payload(left)
                    };
                },
            }
        }
    }

    /// Reads from the socket and parses. `Ready(true)` means something new
    /// arrived and another pass is worth taking.
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        if self.eof {
            return Poll::Ready(Ok(false));
        }

        self.in_raw.reserve(MAX_PAYLOAD as usize);
        let mut buf = ReadBuf::uninit(self.in_raw.spare_capacity_mut());
        match Pin::new(&mut self.io).poll_read(cx, &mut buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len();
                if read == 0 {
                    self.eof = true;
                    // The peer will neither finish an exchange it is part
                    // of nor answer one we start.
                    self.probe.stop();
                } else {
                    // SAFETY: `poll_read` filled exactly this many bytes of the
                    // spare capacity we handed it.
                    unsafe { self.in_raw.set_len(self.in_raw.len() + read) };
                    self.parse()?;
                }
                Poll::Ready(Ok(true))
            },
        }
    }

    /// Everything that has to happen between application calls: start a probe
    /// if one is due, steer the window, return credit, flush.
    fn service(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.shut_down {
            // Nothing more goes on the wire. A reply the parser staged - a
            // `CLEAR_ACK`, a `PONG` - is dropped rather than failing the
            // read that produced it, and no probe is started, since its
            // frames could not be sent either.
            self.out.clear();
            return Ok(());
        }
        if let Some(frame) = self.probe.poll(cx) {
            frame.encode(&mut self.out);
        }
        let rtt = self.probe.clean_rtt();
        self.window.steer(rtt);
        if let Some(granted) = self.window.take_grant(self.in_payload.len(), rtt) {
            Frame::WindowUpdate(granted).encode(&mut self.out);
        }
        match self.poll_send(cx) {
            Poll::Ready(Err(error)) => Err(error),
            _ => Ok(()),
        }
    }

    /// Runs the connection until nothing more can happen without a wake-up.
    fn poll_progress(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        loop {
            self.service(cx)?;
            match self.poll_recv(cx) {
                Poll::Ready(Ok(true)) => {},
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Ready(Ok(false)) | Poll::Pending => break,
            }
        }
        // The last pass through the parser may have freed credit or queued a
        // pong, and neither should wait for the next wake-up.
        self.service(cx)
    }
}

impl<IO> Transit<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// How much of `wanted` bytes the next `DATA` frame may carry, or
    /// `Pending` while nothing may go.
    ///
    /// The whole of what `poll_write` and `poll_write_vectored` share: the two
    /// differ only in where the payload comes from.
    fn poll_frame_room(&mut self, cx: &mut Context<'_>, wanted: usize) -> Poll<io::Result<usize>> {
        self.poll_progress(cx)?;
        if wanted == 0 {
            return Poll::Ready(Ok(0));
        }

        // Each of these is undone by an inbound frame - a window update, a pong
        // - or by the socket draining, and `poll_progress` has just registered
        // for both.
        if self.probe.paused() || self.credit.available == 0 || self.out.len() >= OUT_HIGH_WATER {
            // Only a lack of credit is the window's doing; a paused probe and a
            // full staging buffer are this connection's own cost.
            if self.credit.available == 0 && !self.credit.starved {
                self.credit.starved = true;
                self.credit.unreported = Some(Instant::now());
                self.credit.stalls += 1;
            }
            return Poll::Pending;
        }

        Poll::Ready(Ok(wanted
            .min(MAX_PAYLOAD as usize)
            .min(self.credit.available as usize)))
    }

    /// Stages the header of a `DATA` frame carrying `len` payload bytes, which
    /// the caller appends next, and spends the credit for them.
    fn start_frame(&mut self, len: usize) {
        let stamp = self.started.elapsed().as_micros() as u32;
        let starved = self
            .credit
            .unreported
            .take()
            .is_some_and(|since| since.elapsed() < STALL_REPORT_WINDOW);
        Frame::encode_data(len as u32, starved, stamp, &mut self.out);
        self.credit.available -= len as u64;
    }

    /// Pushes the frame just staged at the socket and reports its payload
    /// length as written.
    ///
    /// Straight at the socket: anything held back here is latency added to
    /// every byte written after it.
    fn finish_frame(&mut self, cx: &mut Context<'_>, len: usize) -> Poll<io::Result<usize>> {
        if let Poll::Ready(Err(error)) = self.poll_send(cx) {
            return Poll::Ready(Err(error));
        }
        Poll::Ready(Ok(len))
    }
}

impl<IO> AsyncRead for Transit<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.poll_progress(cx)?;

        if this.in_payload.is_empty() {
            // An empty read is how `AsyncRead` spells end of stream. Otherwise
            // `poll_recv` has registered with the socket.
            return if this.eof {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            };
        }
        let take = buf.remaining().min(this.in_payload.len());
        buf.put_slice(&this.in_payload[..take]);
        this.in_payload.advance(take);
        Poll::Ready(Ok(()))
    }
}

impl<IO> AsyncWrite for Transit<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let take = std::task::ready!(this.poll_frame_room(cx, buf.len()))?;
        if take == 0 {
            return Poll::Ready(Ok(0));
        }
        this.start_frame(take);
        this.out.extend_from_slice(&buf[..take]);
        this.finish_frame(cx, take)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    /// One frame for the whole batch.
    ///
    /// The slices are coalesced into a single `DATA` frame, up to
    /// [`MAX_PAYLOAD`] and the credit available, so a caller that writes a
    /// batch of small pieces - a multiplexer's frame headers and payloads,
    /// say - pays one header, one timestamp and one `write` for the batch
    /// rather than one of each per piece. Without this the default
    /// implementation would write the first slice alone.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let wanted = bufs.iter().map(|buf| buf.len()).sum();
        let take = std::task::ready!(this.poll_frame_room(cx, wanted))?;
        if take == 0 {
            return Poll::Ready(Ok(0));
        }
        this.start_frame(take);
        let mut left = take;
        for buf in bufs {
            if left == 0 {
                break;
            }
            let n = buf.len().min(left);
            this.out.extend_from_slice(&buf[..n]);
            left -= n;
        }
        this.finish_frame(cx, take)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.poll_progress(cx)?;
        std::task::ready!(this.poll_send(cx))?;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    /// Flushes what is staged and shuts the transport's write side down.
    ///
    /// This end takes no further part in the protocol afterwards; see the
    /// module docs on half-close.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_send(cx))?;
        this.shut_down = true;
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod test {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::*;

    fn config(role: Role, window: u32) -> Config {
        Config {
            sizing: Sizing {
                initial: window,
                max: 1024 * 1024,
                re_grant: window / 4,
                re_grants_per_rtt: 0,
                ..Sizing::default()
            },
            probe_spacing: 4.0,
            role,
        }
    }

    /// Pushes `total` bytes through a pair of connected windows and checks that
    /// every one of them comes out the far end unchanged.
    ///
    /// The window is deliberately far smaller than the transfer, so a run turns
    /// over hundreds of credit loops. A connection that stops granting, or
    /// parks itself with no waker registered, hangs here rather than producing
    /// a wrong answer - which is what the test timeout is for, and is how the
    /// original waker deadlock would have been caught.
    async fn transfer(window: u32, total: usize) -> Stats {
        let (near, far) = duplex(16 * 1024);
        let mut sender = Transit::new(near, config(Role::Initiator, window));
        let mut receiver = Transit::new(far, config(Role::Responder, window));

        // Over a duplex the whole transfer lands inside a single timer tick, so
        // without this the probe schedule never comes due and the run exercises
        // everything except the probe.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let writer = tokio::spawn(async move {
            let chunk = vec![0xAB; 8 * 1024];
            let mut written = 0;
            while written < total {
                let take = chunk.len().min(total - written);
                sender.write_all(&chunk[..take]).await.unwrap();
                written += take;
            }
            sender.shutdown().await.unwrap();
            sender
        });

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).await.unwrap();
        let sender = writer.await.unwrap();

        assert_eq!(received.len(), total, "wrong number of bytes came through");
        assert!(
            received.iter().all(|byte| *byte == 0xAB),
            "payload came through corrupted"
        );
        sender.stats()
    }

    #[tokio::test]
    async fn delivers_everything_through_a_window_far_smaller_than_the_transfer() {
        let stats = transfer(8 * 1024, 512 * 1024).await;
        // A window this small cannot hold the transfer, so the sender must have
        // run out of credit and been given more, repeatedly.
        assert!(stats.stalls > 0, "the window never bound the sender");
    }

    /// The only uninflated round trip comes from the probe, and the rule sizes
    /// from it. This drives an exchange to completion.
    #[tokio::test]
    async fn the_probe_measures_a_round_trip() {
        let stats = transfer(8 * 1024, 512 * 1024).await;
        assert!(stats.probes > 0, "no probe completed");
        assert!(stats.clean_rtt.is_some(), "no round trip was measured");
    }

    /// Shrinking is the path where a re-grant could underflow or a connection
    /// could grant itself to a standstill. Starting oversized makes it shrink.
    #[tokio::test]
    async fn a_transfer_completes_from_an_oversized_window() {
        transfer(256 * 1024, 512 * 1024).await;
    }

    /// The window is the only thing bounding memory on the receiving side, so
    /// a peer that sends past it is a protocol error, not backpressure.
    #[tokio::test]
    async fn a_peer_that_exceeds_its_grant_is_refused() {
        let (mut raw, far) = duplex(1024 * 1024);
        let window = 32 * 1024;
        let mut receiver = Transit::new(far, config(Role::Responder, window));

        // One byte more than was ever granted, in frames the receiver will
        // parse without complaint until the total goes over.
        let mut wire = BytesMut::new();
        Frame::encode_data(window, false, 0, &mut wire);
        wire.extend_from_slice(&vec![0; window as usize]);
        Frame::encode_data(1, false, 0, &mut wire);
        wire.extend_from_slice(&[0]);
        raw.write_all(&wire).await.unwrap();

        let mut sink = vec![0; 2 * window as usize];
        let error = loop {
            match receiver.read(&mut sink).await {
                Ok(0) => panic!("the stream ended without an error"),
                Ok(_) => continue,
                Err(error) => break error,
            }
        };
        assert!(
            error.to_string().contains("granted"),
            "wrong error for an overdrawn window: {error}"
        );
    }

    /// Two initiators would each answer the other's `CLEAR_LINK` and then wait
    /// forever for a `PING`. A configuration mistake should fail, not hang.
    #[tokio::test]
    async fn two_initiators_fail_the_connection_instead_of_hanging() {
        let (near, far) = duplex(16 * 1024);
        let mut left = Transit::new(near, config(Role::Initiator, 8 * 1024));
        let mut right = Transit::new(far, config(Role::Initiator, 8 * 1024));
        tokio::time::sleep(Duration::from_millis(5)).await;

        let writer = tokio::spawn(async move {
            let chunk = [0xAB; 4 * 1024];
            loop {
                if let Err(error) = left.write_all(&chunk).await {
                    return error;
                }
            }
        });
        let mut sink = vec![0; 64 * 1024];
        let error = loop {
            match right.read(&mut sink).await {
                Ok(0) => panic!("the stream ended cleanly with two initiators"),
                Ok(_) => continue,
                Err(error) => break error,
            }
        };
        assert!(
            error.to_string().contains("initiator"),
            "wrong error: {error}"
        );
        assert!(
            writer.await.unwrap().to_string().contains("initiator"),
            "only one side noticed"
        );
    }

    /// A responder whose `PING` never comes must not stay paused forever. With
    /// time paused, the runtime advances straight to the deadline.
    #[tokio::test(start_paused = true)]
    async fn a_responder_gives_up_on_a_probe_that_never_completes() {
        let (mut raw, far) = duplex(64 * 1024);
        let mut responder = Transit::new(far, config(Role::Responder, 32 * 1024));

        // Credit to write with, then a `CLEAR_LINK` with no `PING` behind it.
        let mut wire = BytesMut::new();
        Frame::WindowUpdate(32 * 1024).encode(&mut wire);
        Frame::ClearLink(1).encode(&mut wire);
        raw.write_all(&wire).await.unwrap();

        let started = Instant::now();
        tokio::time::timeout(
            2 * crate::probe::EXCHANGE_DEADLINE,
            responder.write_all(b"after the deadline"),
        )
        .await
        .expect("output stayed paused past the deadline")
        .unwrap();
        assert!(
            started.elapsed() >= crate::probe::EXCHANGE_DEADLINE,
            "output resumed before the deadline, while the probe was still live"
        );
        assert_eq!(responder.stats().probe_timeouts, 1);
    }

    /// A batch of slices goes out as one frame. The default implementation
    /// would write the first slice alone, and a caller that batches a
    /// multiplexer's headers and payloads would pay a frame and a `write` for
    /// each piece.
    #[tokio::test]
    async fn vectored_writes_coalesce_into_one_frame() {
        let (near, far) = duplex(64 * 1024);
        let mut sender = Transit::new(near, config(Role::Initiator, 32 * 1024));
        let mut receiver = Transit::new(far, config(Role::Responder, 32 * 1024));
        assert!(sender.is_write_vectored());

        // Two tasks, because the sender has no credit until the receiver's
        // first poll has put its initial grant on the wire.
        const PIECES: [&[u8]; 3] = [&[1; 8], &[2; 4096], &[3; 8]];
        let writer = tokio::spawn(async move {
            let written = sender
                .write_vectored(&PIECES.map(io::IoSlice::new))
                .await
                .unwrap();
            sender.shutdown().await.unwrap();
            written
        });

        let mut received = Vec::new();
        receiver.read_to_end(&mut received).await.unwrap();
        let written = writer.await.unwrap();
        assert_eq!(
            written,
            8 + 4096 + 8,
            "the batch was not taken in one frame"
        );
        assert_eq!(received, PIECES.concat(), "the batch came through mangled");
    }

    /// Shutting the write side down leaves the read side working, as it does
    /// on a socket. The probe schedule used to keep firing after a shutdown,
    /// and its `CLEAR_LINK` then failed the read that tried to send it.
    #[tokio::test(start_paused = true)]
    async fn reading_continues_after_a_local_shutdown() {
        let (near, far) = duplex(16 * 1024);
        let mut client = Transit::new(near, config(Role::Initiator, 32 * 1024));
        let mut server = Transit::new(far, config(Role::Responder, 32 * 1024));
        tokio::time::sleep(Duration::from_millis(5)).await;

        let reader = tokio::spawn(async move {
            client.write_all(b"hello").await.unwrap();
            client.shutdown().await.unwrap();
            let mut received = Vec::new();
            client.read_to_end(&mut received).await.unwrap();
            (received, client.stats())
        });

        let mut received = Vec::new();
        server.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"hello");
        // Long enough for the client's probe schedule to come due several
        // times over, if it were still running.
        for _ in 0..5 {
            server.write_all(&[7; 1024]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        server.shutdown().await.unwrap();

        let (received, stats) = reader.await.unwrap();
        assert_eq!(received, vec![7; 5 * 1024]);
        assert_eq!(
            stats.probe_timeouts, 0,
            "a probe was started after the shutdown"
        );
    }

    /// A peer that closes mid-exchange is not going to send the `PING`, and
    /// the responder should not stay paused until the deadline waiting for
    /// it. With time paused the runtime would jump straight to that deadline.
    #[tokio::test(start_paused = true)]
    async fn a_peers_eof_ends_the_exchange() {
        let (mut raw, far) = duplex(64 * 1024);
        let mut responder = Transit::new(far, config(Role::Responder, 32 * 1024));

        let mut wire = BytesMut::new();
        Frame::WindowUpdate(32 * 1024).encode(&mut wire);
        Frame::ClearLink(1).encode(&mut wire);
        raw.write_all(&wire).await.unwrap();
        raw.shutdown().await.unwrap();

        let started = Instant::now();
        responder.write_all(b"after the eof").await.unwrap();
        assert!(
            started.elapsed() < crate::probe::EXCHANGE_DEADLINE,
            "output stayed paused until the deadline"
        );
        assert_eq!(
            responder.stats().probe_timeouts,
            0,
            "a peer closing is not a peer failing to answer"
        );
    }

    /// The sender's clock is 32 bits of microseconds and wraps every 71
    /// minutes. Before this was handled, every sample after a wrap read 2^32
    /// µs above the base and the window sat on the floor until the base's
    /// memory ran out.
    #[test]
    fn the_sender_clock_survives_a_wrap() {
        let mut clock = SenderClock::default();
        let before = clock.unwrap(u32::MAX - 10);
        let after = clock.unwrap(5);
        assert_eq!(after - before, 16, "a wrap read as a jump of 2^32");
        assert_eq!(
            clock.unwrap(6) - after,
            1,
            "the lap was not carried past the wrap"
        );
    }

    /// Timestamps ride between a `DATA` header and its payload, so a parser
    /// that miscounts them corrupts the stream rather than failing loudly.
    /// Small writes make every frame a fresh header/timestamp/payload cycle.
    #[tokio::test]
    async fn the_parser_handles_frames_split_across_reads() {
        let (near, far) = duplex(64);
        let mut sender = Transit::new(near, config(Role::Initiator, 32 * 1024));
        let mut receiver = Transit::new(far, config(Role::Responder, 32 * 1024));

        let writer = tokio::spawn(async move {
            for byte in 0..=u8::MAX {
                sender.write_all(&[byte; 7]).await.unwrap();
            }
            sender.shutdown().await.unwrap();
        });
        let mut received = Vec::new();
        receiver.read_to_end(&mut received).await.unwrap();
        writer.await.unwrap();

        let expected: Vec<u8> = (0..=u8::MAX).flat_map(|byte| [byte; 7]).collect();
        assert_eq!(received, expected, "a frame split across reads was mangled");
    }
}
