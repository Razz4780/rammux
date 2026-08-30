use std::{
    collections::VecDeque,
    num::NonZeroU32,
    ops::Not,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::{Instant, Sleep};

use crate::{config::RammuxRole, error::ErrorKind, header::PingPayload};

/// Interval between plain loaded-RTT pings.
const DEFAULT_DIRTY_PING_INTERVAL: Duration = Duration::from_secs(5);

/// Global state shared by all rammux streams within a single rammux connection.
///
/// Used as the polling strategy of the connection's task selector.
pub struct GlobalPool {
    /// Last measured round-trip time of the connection.
    ///
    /// Every sample comes from the link-clearing probe, so it is measured
    /// over a cooperatively drained link: the connection's own standing
    /// queue cannot inflate it.
    pub rtt: Option<Duration>,
    /// Amount of bytes that are currently available in the pool.
    pub available: usize,
    /// The link-clearing probe: rammux's only `PING` mechanism, its RTT
    /// estimator, and its liveness check.
    pub probe: Probe,
    /// In-flight credit granted to us by the peer, if the transit window is enabled.
    pub transit_send: Option<TransitSend>,
    /// State of the transit window we grant to the peer, if enabled.
    pub transit_recv: Option<TransitRecv>,
    /// Loaded RTT of the latest probe exchange: the round trip as it
    /// actually is with the connection's queues standing, rather than
    /// over the drained link. Stream receive windows size from this,
    /// because a stream's credit loop runs through those queues.
    pub dirty_rtt: Option<Duration>,
    /// Set by a stream whose outbound poll was held back solely by an
    /// exhausted transit window.
    ///
    /// Sticky: streams that returned pending on transit credit are not
    /// polled again until a grant wakes them, so the flag has to survive
    /// the passes in between. It is cleared when credit arrives and when
    /// payload is actually emitted.
    pub transit_blocked: bool,
    /// When the current transit stall began, if the sender is stalled.
    pub stalled_since: Option<Instant>,
    /// Total time the sender spent stalled on transit credit.
    pub stalled_total: Duration,
    /// How many transit stalls the sender has entered.
    pub stalled_events: u64,
}

impl Default for GlobalPool {
    fn default() -> Self {
        Self {
            rtt: None,
            available: 0,
            probe: Probe::idle(crate::config::DEFAULT_PING_INTERVAL, RammuxRole::Client),
            transit_send: None,
            transit_recv: None,
            dirty_rtt: None,
            transit_blocked: false,
            stalled_since: None,
            stalled_total: Duration::ZERO,
            stalled_events: 0,
        }
    }
}

impl GlobalPool {
    /// Accounts for one completed pass of the outbound loop.
    ///
    /// `blocked` means the transport was ready to accept more frames and at
    /// least one stream had payload to write, but the transit window was
    /// spent. That is the only state in which the credit-return loop - not
    /// the link, the peer's stream windows, or the probe pause - is what
    /// holds the sender back. Time spent with a merely *fully utilised*
    /// transit window is not a stall: the link is busy draining it.
    pub fn note_outbound_pass(&mut self, blocked: bool) {
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

    /// Returns whether outbound frames other than probe frames
    /// must be held back for the link-clearing probe.
    pub fn probe_paused(&self) -> bool {
        self.probe.paused()
    }

    /// Notifies the transit window that `len` bytes of `DATA` payload were received
    /// and stored in this muxer.
    pub fn transit_recv_freed(&mut self, len: usize) {
        let rtt = self.rtt;
        if let Some(recv) = self.transit_recv.as_mut() {
            let len = u32::try_from(len).unwrap_or(u32::MAX);
            recv.freed = recv.freed.saturating_add(len);
            // Measure the arrival rate over buckets of at least
            // max(2 x RTT, 10ms) - rates sampled over shorter horizons
            // are burst artifacts, not throughput.
            recv.rate_bucket_bytes += u64::from(len);
            let bucket =
                (2 * rtt.unwrap_or(Duration::from_millis(5))).max(Duration::from_millis(10));
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
    }

    /// Produces the next `SESSION_WINDOW_UPDATE` value, if one is due.
    pub fn transit_recv_update(&mut self) -> Option<u32> {
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
        let optimal = match self.rtt {
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

/// The unpaused `PING` that measures the loaded RTT.
///
/// It travels inline with data, so it times the round trip through the
/// queues that are actually standing - which is the yardstick stream
/// receive windows size from. The link-clearing probe measures the other
/// quantity: what the path costs with nothing queued at all.
///
/// It never blocks the probe and the probe never waits on it. The two only
/// interact through the rules that keep a `PING` unambiguous: no probe is
/// initiated while a pong is outstanding, and an inbound `CLEAR_LINK`
/// forgets whatever ping was in flight.
struct DirtyPing {
    /// Elapses when the next ping is due, or when an outstanding one is
    /// given up on. One timer serves both: only one can apply at a time.
    sleep: Pin<Box<Sleep>>,
    interval: Duration,
    state: DirtyPingState,
    /// Produced and waiting to be encoded.
    outbound: Option<PingPayload>,
    /// A payload we stopped waiting for. Its pong may still arrive, and is
    /// then ignored rather than failing the connection.
    forgotten: Option<PingPayload>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DirtyPingState {
    Idle,
    Awaiting {
        payload: PingPayload,
        sent_at: Instant,
    },
}

/// What an inbound `PING` is, given where the probe dance stands.
enum PingKind {
    /// Part of the dance.
    Probe,
    /// The peer's own loaded-RTT ping; answer it and carry on.
    Plain,
    /// The peer owes us something else and cannot legally be pinging.
    Illegal,
}

/// What an inbound `PONG` meant to the plain ping.
enum DirtyPong {
    /// It answered our outstanding ping: the loaded round trip.
    Matched(Duration),
    /// It answered a ping we had already given up on.
    Forgotten,
    /// Not ours - the probe should look at it.
    Other,
}

impl DirtyPing {
    fn new(interval: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(interval)),
            interval,
            state: DirtyPingState::Idle,
            outbound: None,
            forgotten: None,
        }
    }

    /// Queues the next ping when one is due, or gives up on an outstanding
    /// one whose pong never came. `blocked` holds off new pings while a
    /// probe owns the link.
    fn poll(&mut self, cx: &mut Context<'_>, blocked: bool) {
        if self.sleep.poll_unpin(cx).is_pending() {
            return;
        }
        self.sleep.as_mut().reset(Instant::now() + self.interval);
        match self.state {
            DirtyPingState::Awaiting { payload, .. } => {
                self.forgotten = Some(payload);
                self.state = DirtyPingState::Idle;
            },
            DirtyPingState::Idle if !blocked && self.outbound.is_none() => {
                self.outbound = Some(PingPayload::random());
            },
            DirtyPingState::Idle => {},
        }
    }

    /// Hands over a queued ping to be encoded.
    fn take_frame(&mut self) -> Option<PingPayload> {
        let payload = self.outbound.take()?;
        self.state = DirtyPingState::Awaiting {
            payload,
            sent_at: Instant::now(),
        };
        self.sleep.as_mut().reset(Instant::now() + self.interval);
        Some(payload)
    }

    /// Whether a pong is outstanding. A ping that is merely queued does
    /// not count: it has not been sent, so a probe can simply drop it.
    fn awaiting(&self) -> bool {
        matches!(self.state, DirtyPingState::Awaiting { .. })
    }

    /// Forgets whatever is in flight, so a probe can own the link.
    fn cancel(&mut self) {
        self.outbound = None;
        if let DirtyPingState::Awaiting { payload, .. } = self.state {
            self.forgotten = Some(payload);
            self.state = DirtyPingState::Idle;
        }
    }

    fn on_pong(&mut self, payload: PingPayload) -> DirtyPong {
        if let DirtyPingState::Awaiting {
            payload: expected,
            sent_at,
        } = self.state
            && expected == payload
        {
            self.state = DirtyPingState::Idle;
            self.sleep.as_mut().reset(Instant::now() + self.interval);
            return DirtyPong::Matched(sent_at.elapsed());
        }
        if self.forgotten == Some(payload) {
            self.forgotten = None;
            return DirtyPong::Forgotten;
        }
        DirtyPong::Other
    }
}

/// The cooperative link-clearing probe.
///
/// This is the only `PING` mechanism in rammux. Every interval, one side
/// pauses its data output, both sides drain the link with a `CLEAR_LINK`
/// exchange, and each measures RTT with a `PING` exchange over the empty
/// link. A probe that does not complete within one interval kills the
/// connection - it is also the liveness check.
///
/// The dance (I = initiator, R = responder; each direction of the
/// transport is ordered, so a `CLEAR_LINK` frame is a drain barrier).
/// An initiation carries `SYN`; the responder answers with a receipt
/// (`CLEAR_LINK` without `SYN`), which it can only produce after
/// consuming everything ahead of the initiation - receiving it therefore
/// proves both directions are drained:
///
/// ```text
/// I: pause data, send CLEAR_LINK+SYN
/// R: receive CLEAR_LINK+SYN -> pause data, send CLEAR_LINK receipt
/// I: receive receipt (both directions drained) -> send PING
/// R: receive PING (I->R is drained) -> send PONG, own PING, resume data
/// I: receive PONG (clean RTT), receive R's PING -> send PONG, resume data
/// R: receive PONG (clean RTT)
/// ```
///
/// Colliding initiations tie-break by role: the client stays initiator,
/// the server demotes to responder - its own initiation stands as its
/// pause marker and it still sends the receipt. A colliding initiation
/// is no substitute for the receipt: it was sent spontaneously, so it
/// proves nothing about our own direction having drained, and pinging on
/// it would time the residue of our own queue into the sample.
pub struct Probe {
    /// In [`ProbeState::Idle`]: elapses when the next probe is due.
    /// In every other state: elapses when the running probe times out.
    sleep: Pin<Box<Sleep>>,
    /// Interval between probes; also the running probe's timeout.
    interval: Duration,
    /// When the running probe started (left [`ProbeState::Idle`]).
    started_at: Instant,
    /// Protocol state.
    state: ProbeState,
    /// Response owed to the peer's probe `PING`.
    pong: Option<PingPayload>,
    /// Our role, used to tie-break colliding initiations:
    /// the client stays initiator, the server demotes to responder.
    role: RammuxRole,
    /// When this side entered the exchange: our `CLEAR_LINK` went out
    /// (initiator), or the peer's arrived (responder).
    clear_at: Option<Instant>,
    /// Loaded RTT of the most recent exchange, from either the plain ping
    /// or the probe's own `CLEAR_LINK`-to-receipt leg. Drained by
    /// [`Probe::take_dirty_rtt`].
    last_dirty: Option<Duration>,
    /// The plain `PING` that measures the loaded RTT between probes.
    ping: DirtyPing,
    /// Pongs owed to plain pings. The peer keeps one ping in flight, so
    /// this holds one entry in practice.
    plain_pongs: VecDeque<PingPayload>,
}

/// See [`Probe`] doc for the dance these states walk through.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ProbeState {
    /// Connection start. The link is empty by construction - neither
    /// side has sent anything yet - so the opening RTT sample needs no
    /// `CLEAR_LINK` and no pause: both sides simply `PING` as their very
    /// first frame. This is the one exception to the rule that a `PING`
    /// only ever follows a cleared link.
    ///
    /// `peer_pinged`: the peer's opening `PING` arrived before we got to
    /// emit our own - both sides start simultaneously, so either order
    /// happens.
    StartupPing { peer_pinged: bool },
    /// Our opening `PING` is out. Completes like any exchange: our
    /// `PONG` is back and the peer's `PING` has been answered.
    ///
    /// `resumed`: data flows again. That happens as soon as both our
    /// `PING` and the `PONG` we owe the peer have left - from then on
    /// our data can only queue behind them, so neither sample can be
    /// contaminated and there is nothing left to wait for.
    StartupAwaitPong {
        payload: PingPayload,
        sent_at: Instant,
        peer_pinged: bool,
        resumed: bool,
        rtt: Option<Duration>,
    },
    /// No probe in flight; data flows.
    Idle,
    /// Data paused; our `CLEAR_LINK` waits to be sent (`syn` - spontaneous
    /// initiation or receipt), then `next`.
    SendClear { syn: bool, next: AfterClear },
    /// Initiator: `CLEAR_LINK` sent, waiting for the peer's receipt.
    ///
    /// `collided`: the peer initiated simultaneously. As the client we
    /// keep the initiator role, the peer demotes to responder, and its
    /// receipt is still owed - we must not ping before it arrives, since
    /// only the receipt proves our own direction has drained.
    AwaitClear { collided: bool },
    /// Responder: `CLEAR_LINK` sent, waiting for the peer's probe `PING`.
    AwaitPing,
    /// Our probe `PING` waits to be sent over the drained link.
    ///
    /// `resume`: data flow resumes as soon as it is sent (responder side -
    /// its pong and ping lead the resumed data on the drained link).
    /// `peer_pinged`: the peer's own probe `PING` was already received.
    SendPing { resume: bool, peer_pinged: bool },
    /// Probe `PING` sent. The probe completes when the pong has arrived
    /// (`rtt`) and the peer's own probe `PING` has been answered
    /// (`peer_pinged`) - answering it after resuming data would let data
    /// queue in front of our pong and contaminate the peer's sample.
    AwaitPong {
        payload: PingPayload,
        sent_at: Instant,
        resumed: bool,
        peer_pinged: bool,
        rtt: Option<Duration>,
    },
}

/// Successor of [`ProbeState::SendClear`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AfterClear {
    /// We initiated: wait for the peer's receipt.
    AwaitClear { collided: bool },
    /// The peer initiated: wait for the peer's probe `PING`.
    AwaitPing,
}

/// A completed probe.
pub struct ProbeDone {
    /// The clean RTT sample, measured over the drained link.
    pub rtt: Duration,
    /// Whether data flow resumes with this completion
    /// (the caller should wake the streams).
    pub resume: bool,
}

/// A probe frame ready to be encoded.
pub enum ProbeFrame {
    /// `CLEAR_LINK`; `true` marks a spontaneous initiation (`SYN`),
    /// `false` the responder's receipt.
    Clear(bool),
    /// Our probe `PING` request.
    Ping(PingPayload),
    /// Response to the peer's probe `PING`.
    Pong(PingPayload),
}

impl Probe {
    /// Creates a new probe machine.
    ///
    /// Both sides open the connection with a bare `PING` exchange: at
    /// that point neither has sent anything, so the link is empty by
    /// construction and no `CLEAR_LINK` is needed - one round trip
    /// instead of the full clearing dance. Data is still held until the
    /// exchange completes, because the `PONG` we owe the peer must not
    /// queue behind our opening burst - that would hand the peer an
    /// inflated sample, and the transit autotune never gives growth back.
    /// Scheduled probes, which run on a loaded link, still clear it.
    pub fn new(interval: Duration, role: RammuxRole) -> Self {
        Self {
            state: ProbeState::StartupPing { peer_pinged: false },
            ..Self::idle(interval, role)
        }
    }

    /// A probe machine with no opening exchange, for a [`GlobalPool`]
    /// that is not driving a connection.
    fn idle(interval: Duration, role: RammuxRole) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(interval)),
            interval,
            started_at: Instant::now(),
            state: ProbeState::Idle,
            pong: None,
            role,
            clear_at: None,
            last_dirty: None,
            ping: DirtyPing::new(DEFAULT_DIRTY_PING_INTERVAL),
            plain_pongs: VecDeque::new(),
        }
    }

    /// Returns whether outbound frames other than probe frames must be
    /// held back.
    pub fn paused(&self) -> bool {
        match self.state {
            ProbeState::Idle => false,
            ProbeState::AwaitPong { resumed, .. }
            | ProbeState::StartupAwaitPong { resumed, .. } => !resumed,
            _ => true,
        }
    }

    /// Returns whether the peer's data direction is drained: past its
    /// `CLEAR_LINK`, only probe frames may arrive until the probe runs
    /// its course.
    pub fn peer_must_be_silent(&self) -> bool {
        match self.state {
            ProbeState::Idle
            | ProbeState::StartupPing { .. }
            | ProbeState::StartupAwaitPong { .. } => false,
            // Until the peer is known to be in the exchange, its data
            // is legitimate.
            ProbeState::SendClear {
                next: AfterClear::AwaitClear { collided },
                ..
            }
            | ProbeState::AwaitClear { collided } => collided,
            _ => true,
        }
    }

    /// Drives the probe clock: schedules the next probe when due, and
    /// fails when the running probe does not complete within one interval.
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        self.ping.poll(cx, self.paused());
        std::task::ready!(self.sleep.poll_unpin(cx));
        match self.state {
            ProbeState::Idle if self.ping.awaiting() => {
                // A plain ping is still in flight. Starting the probe now
                // would leave its pong indistinguishable from the probe's
                // own, so wait for it to land or be given up on - bounded
                // by the plain ping's own interval.
                self.sleep
                    .as_mut()
                    .reset(Instant::now() + self.ping.interval);
                Poll::Pending
            },
            ProbeState::Idle => {
                // The next probe is due: pause data, queue our CLEAR_LINK.
                self.begin(true, AfterClear::AwaitClear { collided: false });
                Poll::Ready(Ok(()))
            },
            _ => Poll::Ready(Err(ErrorKind::ProbeTimeout {
                elapsed: self.started_at.elapsed(),
            })),
        }
    }

    /// What an inbound `PING` is, given where the dance stands.
    ///
    /// Each direction of the transport is ordered, which is what makes this
    /// decidable without a flag on the wire. Four states are waiting on a
    /// ping from the dance itself. A handful more are states in which the
    /// peer is provably paused and owes us a particular frame, so any ping
    /// is a violation. Everywhere else the peer is free to be running its
    /// own loaded-RTT ping, and that is what this is.
    fn classify_ping(&self) -> PingKind {
        match self.state {
            ProbeState::StartupPing { peer_pinged: false }
            | ProbeState::StartupAwaitPong {
                peer_pinged: false, ..
            }
            | ProbeState::AwaitPing
            | ProbeState::AwaitPong {
                peer_pinged: false, ..
            } => PingKind::Probe,
            // The peer has sent CLEAR_LINK and owes a receipt or a probe
            // ping; it has cancelled its plain ping and paused its data.
            ProbeState::AwaitClear { collided: true }
            | ProbeState::SendPing { .. }
            | ProbeState::StartupPing { peer_pinged: true }
            | ProbeState::StartupAwaitPong {
                peer_pinged: true, ..
            } => PingKind::Illegal,
            // Idle, our own initiation in flight, or a probe the peer has
            // already resumed from: its plain ping can legitimately arrive.
            ProbeState::Idle
            | ProbeState::SendClear { .. }
            | ProbeState::AwaitClear { collided: false }
            | ProbeState::AwaitPong {
                peer_pinged: true, ..
            } => PingKind::Plain,
        }
    }

    /// Takes the most recent loaded-RTT sample, if one has landed.
    pub fn take_dirty_rtt(&mut self) -> Option<Duration> {
        self.last_dirty.take()
    }

    /// Enters a probe: data pauses now, `CLEAR_LINK` goes out next.
    fn begin(&mut self, syn: bool, next: AfterClear) {
        // Any plain ping queued but not yet encoded is dropped: it must not
        // follow our CLEAR_LINK onto the link the probe is clearing.
        self.ping.cancel();
        self.started_at = Instant::now();
        self.sleep.as_mut().reset(self.started_at + self.interval);
        self.state = ProbeState::SendClear { syn, next };
    }

    /// Completes a probe and schedules the next one.
    ///
    /// The next probe is jittered by +-10% so the two peers' probe clocks
    /// do not stay phase-locked (probes re-anchor both sides' timers, so
    /// without jitter, colliding initiations become the steady state).
    fn finish(&mut self, rtt: Duration, resumed: bool) -> Option<ProbeDone> {
        self.state = ProbeState::Idle;
        let jitter = 0.9 + 0.2 * rand::random::<f64>();
        self.sleep
            .as_mut()
            .reset(Instant::now() + self.interval.mul_f64(jitter));
        Some(ProbeDone {
            rtt,
            resume: !resumed,
        })
    }

    /// Handles an inbound `CLEAR_LINK`.
    ///
    /// `syn` marks a spontaneous initiation; without it the frame is the
    /// responder's receipt, sent only in response to our own initiation.
    /// The receipt is the initiator's license to ping: the peer produced
    /// it after consuming everything ahead of our `CLEAR_LINK`, so both
    /// directions are provably drained. A colliding initiation carries no
    /// such proof, which is why the demoted side still owes a receipt.
    pub fn on_clear_link(&mut self, syn: bool) -> Result<(), ErrorKind> {
        // The probe owns the link from here; a plain ping in flight would
        // put an ambiguous pong on it, so it is forgotten.
        self.ping.cancel();
        match (self.state, syn) {
            // The peer initiated: pause data, answer with our receipt,
            // then wait for the peer's probe ping.
            (ProbeState::Idle, true) => {
                self.clear_at = Some(Instant::now());
                self.begin(false, AfterClear::AwaitPing);
                Ok(())
            },
            (ProbeState::Idle, false) => {
                Err(ErrorKind::Probe("CLEAR_LINK receipt without initiation"))
            },
            // Collision: the peer initiated simultaneously. The client
            // stays initiator and waits for the demoted peer's receipt;
            // the server becomes a plain responder - its own initiation
            // stands as its pause marker and the receipt follows it.
            (
                ProbeState::SendClear {
                    syn: true,
                    next: AfterClear::AwaitClear { collided: false },
                },
                true,
            ) => {
                self.state = match self.role {
                    RammuxRole::Client => ProbeState::SendClear {
                        syn: true,
                        next: AfterClear::AwaitClear { collided: true },
                    },
                    RammuxRole::Server => ProbeState::SendClear {
                        syn: false,
                        next: AfterClear::AwaitPing,
                    },
                };
                Ok(())
            },
            (ProbeState::AwaitClear { collided: false }, true) => {
                self.state = match self.role {
                    RammuxRole::Client => ProbeState::AwaitClear { collided: true },
                    RammuxRole::Server => ProbeState::SendClear {
                        syn: false,
                        next: AfterClear::AwaitPing,
                    },
                };
                Ok(())
            },
            // The peer's receipt: both directions are drained, so the ping
            // that follows times the path and nothing else. The time since
            // our CLEAR_LINK went out is the loaded round trip - it carries
            // both sides' standing queues.
            (ProbeState::AwaitClear { .. }, false) => {
                self.last_dirty = self.clear_at.map(|at| at.elapsed());
                self.state = ProbeState::SendPing {
                    resume: false,
                    peer_pinged: false,
                };
                Ok(())
            },
            _ => Err(ErrorKind::Probe("unexpected CLEAR_LINK")),
        }
    }

    /// Handles the peer's probe `PING` request.
    pub fn on_ping(&mut self, payload: PingPayload) -> Result<Option<ProbeDone>, ErrorKind> {
        // Only four states expect a ping from the dance, and each direction
        // of the transport is ordered, so a ping arriving anywhere else is
        // the peer's plain one: answer it and carry on.
        match self.classify_ping() {
            PingKind::Plain => {
                self.plain_pongs.push_back(payload);
                return Ok(None);
            },
            PingKind::Illegal => return Err(ErrorKind::UnexpectedPing(payload)),
            PingKind::Probe => {},
        }
        if self.pong.is_some() {
            return Err(ErrorKind::UnexpectedPing(payload));
        }
        match self.state {
            // The peer's opening ping beat ours out the door.
            ProbeState::StartupPing { peer_pinged: false } => {
                self.pong = Some(payload);
                self.state = ProbeState::StartupPing { peer_pinged: true };
                Ok(None)
            },
            // The peer's opening ping. Answer it; the exchange completes
            // once our own pong is also back.
            ProbeState::StartupAwaitPong {
                payload: ours,
                sent_at,
                peer_pinged: false,
                resumed,
                rtt,
            } => {
                self.pong = Some(payload);
                match rtt {
                    Some(rtt) => Ok(self.finish(rtt, resumed)),
                    None => {
                        self.state = ProbeState::StartupAwaitPong {
                            payload: ours,
                            sent_at,
                            peer_pinged: true,
                            resumed,
                            rtt,
                        };
                        Ok(None)
                    },
                }
            },
            // The peer's probe ping arrived over the drained link: pong
            // it, send our own probe ping, and resume data right after.
            ProbeState::AwaitPing => {
                self.last_dirty = self.clear_at.map(|at| at.elapsed());
                self.pong = Some(payload);
                self.state = ProbeState::SendPing {
                    resume: true,
                    peer_pinged: true,
                };
                Ok(None)
            },
            // Initiator: the responder's own ping, strictly behind its
            // pong on the wire, completes the probe.
            ProbeState::AwaitPong {
                resumed,
                peer_pinged: false,
                rtt: Some(rtt),
                ..
            } => {
                self.pong = Some(payload);
                Ok(self.finish(rtt, resumed))
            },
            _ => Err(ErrorKind::UnexpectedPing(payload)),
        }
    }

    /// Handles the peer's `PING` response to our probe ping.
    pub fn on_pong(&mut self, payload: PingPayload) -> Result<Option<ProbeDone>, ErrorKind> {
        match self.ping.on_pong(payload) {
            DirtyPong::Matched(rtt) => {
                self.last_dirty = Some(rtt);
                return Ok(None);
            },
            // Answering a ping we gave up on, or one cancelled by a probe.
            DirtyPong::Forgotten => return Ok(None),
            DirtyPong::Other => {},
        }
        match self.state {
            ProbeState::StartupAwaitPong {
                payload: expected,
                sent_at,
                peer_pinged,
                resumed,
                rtt: None,
            } if expected == payload => {
                let rtt = sent_at.elapsed();
                if peer_pinged {
                    Ok(self.finish(rtt, resumed))
                } else {
                    self.state = ProbeState::StartupAwaitPong {
                        payload: expected,
                        sent_at,
                        peer_pinged,
                        resumed,
                        rtt: Some(rtt),
                    };
                    Ok(None)
                }
            },
            ProbeState::AwaitPong {
                payload: expected,
                sent_at,
                resumed,
                peer_pinged,
                rtt: None,
            } if expected == payload => {
                let rtt = sent_at.elapsed();
                if peer_pinged {
                    Ok(self.finish(rtt, resumed))
                } else {
                    // The peer's own probe ping is still on its way; hold
                    // the pause until we have answered it.
                    self.state = ProbeState::AwaitPong {
                        payload: expected,
                        sent_at,
                        resumed,
                        peer_pinged,
                        rtt: Some(rtt),
                    };
                    Ok(None)
                }
            },
            _ => Err(ErrorKind::UnexpectedPing(payload)),
        }
    }

    /// Next probe frame to send, if any, and whether sending it resumes
    /// data flow (the caller should wake the streams).
    pub fn next_frame(&mut self) -> Option<(ProbeFrame, bool)> {
        if let Some(payload) = self.pong.take() {
            // Answering the peer's opening ping is the last thing that
            // has to precede our data; once it is out, data resumes.
            let resume = match &mut self.state {
                ProbeState::StartupAwaitPong { resumed, .. } if !*resumed => {
                    *resumed = true;
                    true
                },
                _ => false,
            };
            return Some((ProbeFrame::Pong(payload), resume));
        }
        if let Some(payload) = self.plain_pongs.pop_front() {
            return Some((ProbeFrame::Pong(payload), false));
        }
        if let Some(payload) = self.ping.take_frame() {
            return Some((ProbeFrame::Ping(payload), false));
        }
        match self.state {
            ProbeState::StartupPing { peer_pinged } => {
                let payload = PingPayload::random();
                // If we have already answered the peer's ping, this ping
                // is the last frame that must precede our data.
                self.state = ProbeState::StartupAwaitPong {
                    payload,
                    sent_at: Instant::now(),
                    peer_pinged,
                    resumed: peer_pinged,
                    rtt: None,
                };
                Some((ProbeFrame::Ping(payload), peer_pinged))
            },
            ProbeState::SendClear { syn, next } => {
                if syn {
                    self.clear_at = Some(Instant::now());
                }
                self.state = match next {
                    AfterClear::AwaitClear { collided } => ProbeState::AwaitClear { collided },
                    AfterClear::AwaitPing => ProbeState::AwaitPing,
                };
                Some((ProbeFrame::Clear(syn), false))
            },
            ProbeState::SendPing {
                resume,
                peer_pinged,
            } => {
                let payload = PingPayload::random();
                self.state = ProbeState::AwaitPong {
                    payload,
                    sent_at: Instant::now(),
                    resumed: resume,
                    peer_pinged,
                    rtt: None,
                };
                Some((ProbeFrame::Ping(payload), resume))
            },
            _ => None,
        }
    }
}

/// Sender side of the transit window: how many more bytes of `DATA` payload
/// we may put in flight towards the peer.
pub struct TransitSend {
    pub credit: u32,
}

/// Receiver side of the transit window: tracks credit to return to the peer.
///
/// Credit is freed as soon as `DATA` payload is received and stored in this muxer,
/// independently of when the application reads it.
pub struct TransitRecv {
    /// Current window size.
    pub current: u32,
    /// Bytes freed since the last update.
    pub freed: u32,
    /// Autotune growth limit.
    pub max: u32,
    /// Smoothed arrival rate, bytes per second.
    pub rate_ema: f64,
    /// Start of the current rate-measurement bucket.
    pub rate_bucket_start: Instant,
    /// Payload bytes received in the current rate-measurement bucket.
    pub rate_bucket_bytes: u64,
    /// Time of the last produced window update.
    pub last_update: Instant,
}

impl TransitRecv {
    pub fn new(initial: NonZeroU32, max: u32) -> Self {
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
    use super::*;

    fn new_probe(role: RammuxRole) -> Probe {
        Probe::new(Duration::from_secs(5), role)
    }

    fn poll_probe(probe: &mut Probe) -> Poll<Result<(), ErrorKind>> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        probe.poll(&mut cx)
    }

    /// Waits out the initial interval and starts the first probe.
    async fn start_probe(probe: &mut Probe) {
        settle_startup(probe).await;
        assert!(matches!(poll_probe(probe), Poll::Pending));
        // The next probe is jittered, so advance past the widest spacing.
        tokio::time::advance(2 * probe.interval).await;
        assert!(matches!(poll_probe(probe), Poll::Ready(Ok(()))));
    }

    /// Drives the bare opening `PING` exchange to completion.
    async fn settle_startup(probe: &mut Probe) {
        assert!(probe.paused(), "the opening exchange holds data back");
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", false)));
        let ProbeState::StartupAwaitPong { payload, .. } = probe.state else {
            panic!("expected StartupAwaitPong");
        };
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        assert!(probe.paused(), "still held: the owed pong has not left yet");
        assert_eq!(
            frame_kind(probe.next_frame()),
            Some(("pong", true)),
            "the owed pong releases the pause"
        );
        assert!(
            !probe.paused(),
            "data resumes without waiting for our own pong"
        );
        let done = probe.on_pong(payload).unwrap().unwrap();
        assert!(!done.resume, "already resumed at the pong");
        assert!(matches!(probe.state, ProbeState::Idle));
    }

    #[tokio::test(start_paused = true)]
    async fn startup_is_a_bare_ping_exchange() {
        for role in [RammuxRole::Client, RammuxRole::Server] {
            let mut probe = new_probe(role);
            settle_startup(&mut probe).await;
            assert!(!probe.paused());
        }
        // ...and it is a PING exchange, never a CLEAR_LINK one.
    }

    #[tokio::test(start_paused = true)]
    async fn startup_timeout_kills() {
        let mut probe = new_probe(RammuxRole::Client);
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", false)));
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(matches!(
            poll_probe(&mut probe),
            Poll::Ready(Err(ErrorKind::ProbeTimeout { .. }))
        ));
    }

    fn frame_kind(frame: Option<(ProbeFrame, bool)>) -> Option<(&'static str, bool)> {
        frame.map(|(frame, resume)| {
            let kind = match frame {
                ProbeFrame::Clear(true) => "clear+syn",
                ProbeFrame::Clear(false) => "clear",
                ProbeFrame::Ping(..) => "ping",
                ProbeFrame::Pong(..) => "pong",
            };
            (kind, resume)
        })
    }

    #[tokio::test(start_paused = true)]
    async fn initiator_flow() {
        let mut probe = new_probe(RammuxRole::Client);
        start_probe(&mut probe).await;
        assert!(probe.paused());
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
        assert!(frame_kind(probe.next_frame()).is_none());
        // The peer's receipt arrives: both directions drained, ping.
        probe.on_clear_link(false).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", false)));
        assert!(probe.paused());
        let ProbeState::AwaitPong { payload, .. } = probe.state else {
            panic!("expected AwaitPong");
        };
        // Pong arrives first, then the peer's own ping (wire order).
        tokio::time::advance(Duration::from_millis(40)).await;
        assert!(probe.on_pong(payload).unwrap().is_none());
        assert!(
            probe.paused(),
            "pause holds until the peer's ping is answered"
        );
        let done = probe.on_ping(PingPayload::random()).unwrap().unwrap();
        assert_eq!(done.rtt, Duration::from_millis(40));
        assert!(done.resume);
        assert!(!probe.paused());
        // The owed pong goes out before resumed data.
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        assert!(frame_kind(probe.next_frame()).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn responder_flow() {
        let mut probe = new_probe(RammuxRole::Server);
        settle_startup(&mut probe).await;
        // The peer initiates; we answer with a receipt.
        probe.on_clear_link(true).unwrap();
        assert!(probe.paused());
        assert!(probe.peer_must_be_silent());
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        // The peer's probe ping arrives over the drained link.
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        // Pong leads, then our own ping resumes data.
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", true)));
        assert!(!probe.paused());
        let ProbeState::AwaitPong { payload, .. } = probe.state else {
            panic!("expected AwaitPong");
        };
        let done = probe.on_pong(payload).unwrap().unwrap();
        assert!(!done.resume, "responder resumed at ping send already");
    }

    /// A peer's probe re-anchors our own timer, so the two do not run
    /// back-to-back: after answering one, the next spontaneous probe is a
    /// full jittered interval away rather than whatever was left on the
    /// clock when the peer interrupted us.
    #[tokio::test(start_paused = true)]
    async fn peer_probe_resets_our_timer() {
        // Production intervals: the probe has to be the slower of the two,
        // or deferring it behind a plain ping would have nothing to defer to.
        let mut probe = Probe::new(Duration::from_secs(20), RammuxRole::Server);
        settle_startup(&mut probe).await;

        // Sit most of the way to our own deadline, then let the peer go first.
        tokio::time::advance(probe.interval.mul_f64(0.9)).await;
        probe.on_clear_link(true).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", true)));
        let ProbeState::AwaitPong { payload, .. } = probe.state else {
            panic!("expected AwaitPong");
        };
        probe.on_pong(payload).unwrap().unwrap();
        assert_eq!(probe.state, ProbeState::Idle);

        // The 10% that was left on the old clock must not fire a probe.
        // Plain pings keep running, so only a CLEAR_LINK would be wrong.
        tokio::time::advance(probe.interval.mul_f64(0.5)).await;
        assert!(poll_probe(&mut probe).is_pending());
        assert!(
            !matches!(probe.next_frame(), Some((ProbeFrame::Clear(_), _))),
            "probe ran back-to-back"
        );

        // A full interval past completion, past the +10% jitter bound, it does.
        tokio::time::advance(probe.interval.mul_f64(0.65)).await;
        assert!(matches!(poll_probe(&mut probe), Poll::Ready(Ok(()))));
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
    }

    /// A probe is not initiated while a plain ping is unanswered: its pong
    /// would be indistinguishable from the probe's own.
    #[tokio::test(start_paused = true)]
    async fn probe_waits_for_an_outstanding_pong() {
        let mut probe = Probe::new(Duration::from_secs(20), RammuxRole::Client);
        settle_startup(&mut probe).await;

        // Let the plain ping go out and leave it unanswered.
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(poll_probe(&mut probe).is_pending());
        let Some((ProbeFrame::Ping(outstanding), _)) = probe.next_frame() else {
            panic!("expected a plain ping");
        };

        // Bring the probe due while that pong is still outstanding - in a
        // run it is what happens whenever the probe's turn lands inside the
        // few milliseconds a ping is in flight.
        probe.sleep.as_mut().reset(Instant::now());
        assert!(poll_probe(&mut probe).is_pending());
        assert!(
            !matches!(probe.next_frame(), Some((ProbeFrame::Clear(_), _))),
            "probe initiated with a pong outstanding"
        );

        // Once the pong lands it measures the loaded RTT, and the probe is
        // free to run at the next opportunity.
        assert!(probe.on_pong(outstanding).unwrap().is_none());
        assert!(probe.take_dirty_rtt().is_some());
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(matches!(poll_probe(&mut probe), Poll::Ready(Ok(()))));
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
    }

    /// An inbound `CLEAR_LINK` forgets a ping in flight, and the pong that
    /// eventually answers it is ignored rather than failing the connection.
    #[tokio::test(start_paused = true)]
    async fn clear_link_forgets_a_ping_in_flight() {
        let mut probe = Probe::new(Duration::from_secs(20), RammuxRole::Server);
        settle_startup(&mut probe).await;

        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(poll_probe(&mut probe).is_pending());
        let Some((ProbeFrame::Ping(abandoned), _)) = probe.next_frame() else {
            panic!("expected a plain ping");
        };

        // The peer initiates: our ping is forgotten and we answer normally.
        probe.on_clear_link(true).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));

        // The late pong is neither an error nor a loaded-RTT sample.
        assert!(probe.on_pong(abandoned).unwrap().is_none());
        assert!(
            probe.take_dirty_rtt().is_none(),
            "a forgotten ping must not produce a sample"
        );
        // A pong for something never sent is still a violation.
        assert!(probe.on_pong(PingPayload::random()).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn collision_client_waits_for_receipt() {
        let mut probe = new_probe(RammuxRole::Client);
        start_probe(&mut probe).await;
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
        // The peer's colliding initiation is no license to ping: it
        // proves nothing about our own direction having drained.
        probe.on_clear_link(true).unwrap();
        assert!(probe.peer_must_be_silent());
        assert!(
            frame_kind(probe.next_frame()).is_none(),
            "no ping before the receipt"
        );
        // The demoted server's receipt arrives: now we ping.
        probe.on_clear_link(false).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", false)));
    }

    #[tokio::test(start_paused = true)]
    async fn collision_server_demotes() {
        let mut probe = new_probe(RammuxRole::Server);
        start_probe(&mut probe).await;
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
        // The client's colliding initiation demotes us to responder:
        // we owe a receipt and wait for the client's ping.
        probe.on_clear_link(true).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        assert!(matches!(probe.state, ProbeState::AwaitPing));
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", true)));
    }

    #[tokio::test(start_paused = true)]
    async fn collision_server_demotes_before_send() {
        let mut probe = new_probe(RammuxRole::Server);
        start_probe(&mut probe).await;
        // The client's initiation arrives before our own went out: our
        // queued CLEAR_LINK becomes the receipt - a single frame.
        probe.on_clear_link(true).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        assert!(matches!(probe.state, ProbeState::AwaitPing));
        assert!(frame_kind(probe.next_frame()).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_kills() {
        let mut probe = new_probe(RammuxRole::Client);
        start_probe(&mut probe).await;
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(matches!(
            poll_probe(&mut probe),
            Poll::Ready(Err(ErrorKind::ProbeTimeout { .. }))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn strictness() {
        let mut probe = new_probe(RammuxRole::Server);
        settle_startup(&mut probe).await;
        // A bare ping is the peer's loaded-RTT ping and is answered; a bare
        // pong answers nothing we sent, and a receipt without an initiation
        // is still a violation.
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        assert_eq!(
            frame_kind(probe.next_frame()),
            Some(("pong", false)),
            "a plain ping is answered"
        );
        assert!(probe.on_pong(PingPayload::random()).is_err());
        assert!(probe.on_clear_link(false).is_err());
        // Responder cannot receive another CLEAR_LINK of either kind.
        probe.on_clear_link(true).unwrap();
        probe.next_frame();
        assert!(probe.on_clear_link(true).is_err());
        assert!(probe.on_clear_link(false).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn strictness_collided_client() {
        let mut probe = new_probe(RammuxRole::Client);
        start_probe(&mut probe).await;
        probe.next_frame();
        probe.on_clear_link(true).unwrap();
        // A second colliding initiation is a violation.
        assert!(probe.on_clear_link(true).is_err());
        // The peer's ping cannot legally precede our own.
        let ProbeState::AwaitClear { collided: true } = probe.state else {
            panic!("expected collided AwaitClear");
        };
        assert!(probe.on_ping(PingPayload::random()).is_err());
    }
}
