//! RTT measurement: the link-clearing probe and the loaded-RTT ping.
//!
//! rammux measures two different round trips, with two different `PING`
//! mechanisms:
//!
//! - The **link-clearing probe** ([`Probe::start`]) pauses data output on
//!   both sides, drains the link with a `CLEAR_LINK` exchange, and then
//!   times a `PING` over the empty link. That sample is the path's own
//!   cost, which the connection's standing queues cannot inflate.
//! - The **plain ping** ([`Probe::send_ping`]) travels inline with data
//!   and times the round trip through the queues that are actually
//!   standing. That is the yardstick stream receive windows size from,
//!   because a stream's credit loop runs through those same queues.
//!
//! Neither mechanism runs on a clock. Nothing here sleeps, wakes a task,
//! or gives up on an exchange: both are started by the caller, and every
//! transition is announced as a [`ProbeEvent`] so the caller can impose
//! whatever schedule and whatever deadlines it wants. See
//! [`RammuxConnection`](crate::connection::RammuxConnection) for the
//! caller-facing API.

use std::{collections::VecDeque, ops::Not, time::Duration};

use tokio::time::Instant;

use crate::{config::RammuxRole, error::ErrorKind, header::PingPayload};

/// A transition in a connection's RTT-measurement state machine.
///
/// rammux runs no timers: the link-clearing probe and the loaded-RTT ping
/// are started by the caller, and nothing in this crate ever gives up on
/// one. These events are what an observer needs to do that itself. Every
/// exchange is announced when it starts and again when it ends:
///
/// | start | end |
/// | --- | --- |
/// | [`ProbeEvent::PingSent`] | [`ProbeEvent::PingAnswered`] or [`ProbeEvent::PingAbandoned`] |
/// | [`ProbeEvent::ProbeStarted`] | [`ProbeEvent::ProbeCompleted`] |
///
/// An unfinished probe pauses this side's data output indefinitely, so
/// putting a deadline on [`ProbeEvent::ProbeStarted`] is how an
/// application detects a dead peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProbeEvent {
    /// A plain `PING` requested with
    /// [`RammuxConnection::send_ping`](crate::connection::RammuxConnection::send_ping)
    /// has been encoded, and its round trip is now being timed.
    PingSent,
    /// The peer answered the plain `PING`.
    PingAnswered {
        /// The loaded round trip: the path plus both sides' standing queues.
        rtt: Duration,
    },
    /// The plain `PING` was given up on, either because
    /// [`RammuxConnection::abandon_ping`](crate::connection::RammuxConnection::abandon_ping)
    /// was called or because an inbound `CLEAR_LINK` took the link for a
    /// probe. A pong that arrives for it afterwards is ignored.
    PingAbandoned,
    /// A link-clearing probe has begun and data output is now paused.
    ProbeStarted {
        /// Whether this side initiated the probe. `false` means the peer
        /// did, and this side was pulled into the exchange by an inbound
        /// `CLEAR_LINK` - which happens regardless of the local schedule.
        initiated: bool,
    },
    /// The link-clearing probe completed and data output has resumed.
    ProbeCompleted {
        /// The clean round trip, measured over the drained link.
        rtt: Duration,
    },
}

/// The plain `PING` that measures the loaded RTT.
///
/// It never blocks the probe and the probe never waits on it. The two only
/// interact through the rules that keep a `PING` unambiguous: no probe is
/// initiated while a pong is outstanding, and an inbound `CLEAR_LINK`
/// forgets whatever ping was in flight.
struct DirtyPing {
    state: DirtyPingState,
    /// A payload we stopped waiting for. Its pong may still arrive, and is
    /// then ignored rather than failing the connection.
    forgotten: Option<PingPayload>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DirtyPingState {
    /// No ping requested.
    Idle,
    /// Requested, waiting to be encoded.
    Queued { payload: PingPayload },
    /// On the wire, being timed.
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
    fn new() -> Self {
        Self {
            state: DirtyPingState::Idle,
            forgotten: None,
        }
    }

    /// Queues a ping. Returns `false` if one is already queued or in flight:
    /// only one plain ping is ever outstanding, so its pong is unambiguous.
    fn send(&mut self) -> bool {
        if matches!(self.state, DirtyPingState::Idle).not() {
            return false;
        }
        self.state = DirtyPingState::Queued {
            payload: PingPayload::random(),
        };
        true
    }

    /// Hands over a queued ping to be encoded.
    fn take_frame(&mut self) -> Option<PingPayload> {
        let DirtyPingState::Queued { payload } = self.state else {
            return None;
        };
        self.state = DirtyPingState::Awaiting {
            payload,
            sent_at: Instant::now(),
        };
        Some(payload)
    }

    /// Whether a pong is outstanding. A ping that is merely queued does
    /// not count: it has not been sent, so a probe can simply drop it.
    fn awaiting(&self) -> bool {
        matches!(self.state, DirtyPingState::Awaiting { .. })
    }

    /// Gives up on the ping, so a probe can own the link.
    ///
    /// Returns whether one was actually in flight. A queued ping is
    /// dropped either way, but it never reached the wire, so there is
    /// nothing to announce and nothing to ignore later.
    fn abandon(&mut self) -> bool {
        match std::mem::replace(&mut self.state, DirtyPingState::Idle) {
            DirtyPingState::Awaiting { payload, .. } => {
                self.forgotten = Some(payload);
                true
            },
            DirtyPingState::Idle | DirtyPingState::Queued { .. } => false,
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
/// One side pauses its data output, both sides drain the link with a
/// `CLEAR_LINK` exchange, and each measures RTT with a `PING` exchange
/// over the empty link.
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
///
/// This type also owns the plain ping, because the two mechanisms share
/// the `PING` frame and have to stay mutually unambiguous.
pub struct Probe {
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
    /// The plain `PING` that measures the loaded RTT.
    ping: DirtyPing,
    /// Pongs owed to plain pings. The peer keeps one ping in flight, so
    /// this holds one entry in practice.
    plain_pongs: VecDeque<PingPayload>,
    /// Transitions not yet handed to the caller.
    events: VecDeque<ProbeEvent>,
}

/// See [`Probe`] doc for the dance these states walk through.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ProbeState {
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
    /// A `PING` request, from either mechanism.
    Ping(PingPayload),
    /// Response to the peer's `PING`.
    Pong(PingPayload),
}

impl Probe {
    /// Creates an idle probe machine.
    ///
    /// It stays idle until [`Probe::start`] or [`Probe::send_ping`] is
    /// called, or the peer initiates a probe of its own.
    pub fn new(role: RammuxRole) -> Self {
        Self {
            state: ProbeState::Idle,
            pong: None,
            role,
            clear_at: None,
            last_dirty: None,
            ping: DirtyPing::new(),
            plain_pongs: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    /// Initiates a link-clearing probe: data pauses now and `CLEAR_LINK`
    /// goes out with the next frame.
    ///
    /// Returns whether the probe was started. It is refused while another
    /// probe is running, and while a plain ping is unanswered - that
    /// ping's pong would be indistinguishable from the probe's own.
    pub fn start(&mut self) -> bool {
        if matches!(self.state, ProbeState::Idle).not() || self.ping.awaiting() {
            return false;
        }
        self.begin(true, AfterClear::AwaitClear { collided: false });
        true
    }

    /// Queues a plain `PING` to measure the loaded RTT.
    ///
    /// Returns whether it was queued. It is refused while a probe is
    /// running, since a `PING` there belongs to the dance, and while
    /// another plain ping is queued or in flight.
    pub fn send_ping(&mut self) -> bool {
        matches!(self.state, ProbeState::Idle) && self.ping.send()
    }

    /// Gives up on the plain ping, freeing the probe to run.
    ///
    /// Returns whether one was in flight. A pong that arrives for it
    /// afterwards is ignored rather than failing the connection.
    pub fn abandon_ping(&mut self) -> bool {
        let was_in_flight = self.ping.abandon();
        if was_in_flight {
            self.events.push_back(ProbeEvent::PingAbandoned);
        }
        was_in_flight
    }

    /// Takes the next transition to report to the caller.
    pub fn next_event(&mut self) -> Option<ProbeEvent> {
        self.events.pop_front()
    }

    /// Returns whether outbound frames other than probe frames must be
    /// held back.
    pub fn paused(&self) -> bool {
        match self.state {
            ProbeState::Idle => false,
            ProbeState::AwaitPong { resumed, .. } => !resumed,
            _ => true,
        }
    }

    /// Returns whether the peer's data direction is drained: past its
    /// `CLEAR_LINK`, only probe frames may arrive until the probe runs
    /// its course.
    pub fn peer_must_be_silent(&self) -> bool {
        match self.state {
            ProbeState::Idle => false,
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

    /// What an inbound `PING` is, given where the dance stands.
    ///
    /// Each direction of the transport is ordered, which is what makes this
    /// decidable without a flag on the wire. Two states are waiting on a
    /// ping from the dance itself. A couple more are states in which the
    /// peer is provably paused and owes us a particular frame, so any ping
    /// is a violation. Everywhere else the peer is free to be running its
    /// own loaded-RTT ping, and that is what this is.
    fn classify_ping(&self) -> PingKind {
        match self.state {
            ProbeState::AwaitPing
            | ProbeState::AwaitPong {
                peer_pinged: false, ..
            } => PingKind::Probe,
            // The peer has sent CLEAR_LINK and owes a receipt or a probe
            // ping; it has given up its plain ping and paused its data.
            ProbeState::AwaitClear { collided: true } | ProbeState::SendPing { .. } => {
                PingKind::Illegal
            },
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
    ///
    /// `syn` distinguishes our own initiation from the receipt we owe a
    /// peer that got there first, which is also what makes it the
    /// `initiated` flag of the announced transition.
    fn begin(&mut self, syn: bool, next: AfterClear) {
        // Any plain ping is dropped: queued, it must not follow our
        // CLEAR_LINK onto the link the probe is clearing; in flight, its
        // pong would be indistinguishable from the probe's own.
        self.abandon_ping();
        self.state = ProbeState::SendClear { syn, next };
        self.events
            .push_back(ProbeEvent::ProbeStarted { initiated: syn });
    }

    /// Completes a probe.
    fn finish(&mut self, rtt: Duration, resumed: bool) -> Option<ProbeDone> {
        self.state = ProbeState::Idle;
        self.events.push_back(ProbeEvent::ProbeCompleted { rtt });
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
        self.abandon_ping();
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

    /// Handles an inbound `PING` request.
    pub fn on_ping(&mut self, payload: PingPayload) -> Result<Option<ProbeDone>, ErrorKind> {
        // Only two states expect a ping from the dance, and each direction
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

    /// Handles an inbound `PING` response.
    pub fn on_pong(&mut self, payload: PingPayload) -> Result<Option<ProbeDone>, ErrorKind> {
        match self.ping.on_pong(payload) {
            DirtyPong::Matched(rtt) => {
                self.last_dirty = Some(rtt);
                self.events.push_back(ProbeEvent::PingAnswered { rtt });
                return Ok(None);
            },
            // Answering a ping we gave up on, or one dropped for a probe.
            DirtyPong::Forgotten => return Ok(None),
            DirtyPong::Other => {},
        }
        match self.state {
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

    /// Next frame to send, if any, and whether sending it resumes data
    /// flow (the caller should wake the streams).
    pub fn next_frame(&mut self) -> Option<(ProbeFrame, bool)> {
        if let Some(payload) = self.pong.take() {
            return Some((ProbeFrame::Pong(payload), false));
        }
        if let Some(payload) = self.plain_pongs.pop_front() {
            return Some((ProbeFrame::Pong(payload), false));
        }
        if let Some(payload) = self.ping.take_frame() {
            self.events.push_back(ProbeEvent::PingSent);
            return Some((ProbeFrame::Ping(payload), false));
        }
        match self.state {
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

#[cfg(test)]
mod test {
    use super::*;

    /// Compact rendering of [`Probe::next_frame`]: the frame's kind and
    /// whether sending it resumes data flow.
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

    fn events(probe: &mut Probe) -> Vec<ProbeEvent> {
        std::iter::from_fn(|| probe.next_event()).collect()
    }

    /// A probe machine is idle until something starts an exchange: it has
    /// no clock of its own, so an unpolled one never sends anything.
    #[test]
    fn starts_idle_and_silent() {
        for role in [RammuxRole::Client, RammuxRole::Server] {
            let mut probe = Probe::new(role);
            assert!(!probe.paused());
            assert!(!probe.peer_must_be_silent());
            assert!(frame_kind(probe.next_frame()).is_none());
            assert!(events(&mut probe).is_empty());
        }
    }

    #[test]
    fn initiator_flow() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.start());
        assert_eq!(
            events(&mut probe),
            [ProbeEvent::ProbeStarted { initiated: true }]
        );
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
        assert!(probe.on_pong(payload).unwrap().is_none());
        assert!(
            probe.paused(),
            "pause holds until the peer's ping is answered"
        );
        let done = probe.on_ping(PingPayload::random()).unwrap().unwrap();
        assert!(done.resume);
        assert!(!probe.paused());
        assert_eq!(
            events(&mut probe),
            [ProbeEvent::ProbeCompleted { rtt: done.rtt }]
        );
        // The owed pong goes out before resumed data.
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        assert!(frame_kind(probe.next_frame()).is_none());
    }

    /// The clean RTT is the time our own probe `PING` spent on the wire.
    #[tokio::test(start_paused = true)]
    async fn initiator_times_the_drained_ping() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.start());
        probe.next_frame();
        probe.on_clear_link(false).unwrap();
        probe.next_frame();
        let ProbeState::AwaitPong { payload, .. } = probe.state else {
            panic!("expected AwaitPong");
        };
        tokio::time::advance(Duration::from_millis(40)).await;
        probe.on_pong(payload).unwrap();
        let done = probe.on_ping(PingPayload::random()).unwrap().unwrap();
        assert_eq!(done.rtt, Duration::from_millis(40));
    }

    #[test]
    fn responder_flow() {
        let mut probe = Probe::new(RammuxRole::Server);
        // The peer initiates; we answer with a receipt.
        probe.on_clear_link(true).unwrap();
        assert_eq!(
            events(&mut probe),
            [ProbeEvent::ProbeStarted { initiated: false }],
            "a peer-initiated probe pauses our data too, so it is announced"
        );
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
        assert_eq!(
            events(&mut probe),
            [ProbeEvent::ProbeCompleted { rtt: done.rtt }]
        );
    }

    /// A plain ping is a self-contained exchange that leaves the probe
    /// machine alone: it just goes out and comes back with a loaded sample.
    #[tokio::test(start_paused = true)]
    async fn plain_ping_measures_the_loaded_rtt() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.send_ping());
        assert!(events(&mut probe).is_empty(), "nothing has left yet");
        assert!(!probe.paused(), "a plain ping travels inline with data");
        let Some((ProbeFrame::Ping(payload), false)) = probe.next_frame() else {
            panic!("expected a plain ping");
        };
        assert_eq!(events(&mut probe), [ProbeEvent::PingSent]);
        assert!(
            !probe.send_ping(),
            "only one plain ping is outstanding at a time"
        );

        tokio::time::advance(Duration::from_millis(80)).await;
        assert!(probe.on_pong(payload).unwrap().is_none());
        let rtt = Duration::from_millis(80);
        assert_eq!(events(&mut probe), [ProbeEvent::PingAnswered { rtt }]);
        assert_eq!(probe.take_dirty_rtt(), Some(rtt));
        assert!(probe.send_ping(), "the next one is free to go");
    }

    /// The peer's plain ping is answered wherever it can legally arrive.
    #[test]
    fn plain_ping_is_answered() {
        let mut probe = Probe::new(RammuxRole::Server);
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        assert!(
            events(&mut probe).is_empty(),
            "answering is not a transition"
        );
    }

    /// A probe is not initiated while a plain ping is unanswered: its pong
    /// would be indistinguishable from the probe's own. The caller decides
    /// what to do about it - wait, or give the ping up.
    #[test]
    fn probe_refuses_to_run_with_a_pong_outstanding() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.send_ping());
        let Some((ProbeFrame::Ping(outstanding), _)) = probe.next_frame() else {
            panic!("expected a plain ping");
        };

        assert!(!probe.start(), "probe initiated with a pong outstanding");
        assert!(frame_kind(probe.next_frame()).is_none());

        // Once the pong lands the probe is free to run.
        assert!(probe.on_pong(outstanding).unwrap().is_none());
        assert!(probe.start());
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
    }

    /// Giving up on the ping unblocks the probe immediately, and the pong
    /// that eventually answers it is ignored rather than failing the
    /// connection.
    #[test]
    fn abandoning_a_ping_unblocks_the_probe() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.send_ping());
        let Some((ProbeFrame::Ping(abandoned), _)) = probe.next_frame() else {
            panic!("expected a plain ping");
        };
        assert_eq!(events(&mut probe), [ProbeEvent::PingSent]);

        assert!(probe.abandon_ping());
        assert!(!probe.abandon_ping(), "nothing left to give up on");
        assert_eq!(events(&mut probe), [ProbeEvent::PingAbandoned]);

        assert!(probe.start());
        assert!(probe.on_pong(abandoned).unwrap().is_none());
        assert!(
            probe.take_dirty_rtt().is_none(),
            "a forgotten ping must not produce a sample"
        );
    }

    /// A queued ping that never reached the wire is dropped silently:
    /// there is nothing outstanding to announce or to ignore later.
    #[test]
    fn abandoning_a_queued_ping_is_silent() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.send_ping());
        assert!(!probe.abandon_ping());
        assert!(events(&mut probe).is_empty());
        assert!(frame_kind(probe.next_frame()).is_none(), "ping was dropped");
    }

    /// An inbound `CLEAR_LINK` gives up a ping in flight: the probe owns
    /// the link from there, and an ambiguous pong must not be on it.
    #[test]
    fn clear_link_forgets_a_ping_in_flight() {
        let mut probe = Probe::new(RammuxRole::Server);
        assert!(probe.send_ping());
        let Some((ProbeFrame::Ping(abandoned), _)) = probe.next_frame() else {
            panic!("expected a plain ping");
        };
        assert_eq!(events(&mut probe), [ProbeEvent::PingSent]);

        // The peer initiates: our ping is forgotten and we answer normally.
        probe.on_clear_link(true).unwrap();
        assert_eq!(
            events(&mut probe),
            [
                ProbeEvent::PingAbandoned,
                ProbeEvent::ProbeStarted { initiated: false }
            ]
        );
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

    /// A plain ping cannot be started under a running probe: on the wire
    /// it would be indistinguishable from the dance's own `PING`.
    #[test]
    fn plain_ping_is_refused_under_a_probe() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.start());
        assert!(!probe.send_ping());
        probe.next_frame();
        probe.on_clear_link(false).unwrap();
        assert!(!probe.send_ping());
        probe.next_frame();
        assert!(!probe.send_ping());
    }

    /// The probe's own `CLEAR_LINK`-to-receipt leg is a loaded sample: it
    /// crosses the link with both sides' queues still standing.
    #[tokio::test(start_paused = true)]
    async fn probe_measures_the_loaded_leg_too() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.start());
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear+syn", false)));
        tokio::time::advance(Duration::from_millis(120)).await;
        probe.on_clear_link(false).unwrap();
        assert_eq!(probe.take_dirty_rtt(), Some(Duration::from_millis(120)));
    }

    #[test]
    fn collision_client_waits_for_receipt() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.start());
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
        assert_eq!(
            events(&mut probe),
            [ProbeEvent::ProbeStarted { initiated: true }],
            "a collision is one probe, not two"
        );
    }

    #[test]
    fn collision_server_demotes() {
        let mut probe = Probe::new(RammuxRole::Server);
        assert!(probe.start());
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

    #[test]
    fn collision_server_demotes_before_send() {
        let mut probe = Probe::new(RammuxRole::Server);
        assert!(probe.start());
        // The client's initiation arrives before our own went out: our
        // queued CLEAR_LINK becomes the receipt - a single frame.
        probe.on_clear_link(true).unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        assert!(matches!(probe.state, ProbeState::AwaitPing));
        assert!(frame_kind(probe.next_frame()).is_none());
    }

    #[test]
    fn strictness() {
        let mut probe = Probe::new(RammuxRole::Server);
        // A bare pong answers nothing we sent, and a receipt without an
        // initiation is a violation.
        assert!(probe.on_pong(PingPayload::random()).is_err());
        assert!(probe.on_clear_link(false).is_err());
        // A running probe cannot be started again.
        probe.on_clear_link(true).unwrap();
        assert!(!probe.start());
        probe.next_frame();
        // Responder cannot receive another CLEAR_LINK of either kind.
        assert!(probe.on_clear_link(true).is_err());
        assert!(probe.on_clear_link(false).is_err());
    }

    #[test]
    fn strictness_collided_client() {
        let mut probe = Probe::new(RammuxRole::Client);
        assert!(probe.start());
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
