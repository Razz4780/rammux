use std::{
    num::NonZeroU32,
    ops::Not,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::{Instant, Sleep};

use crate::{config::RammuxRole, error::ErrorKind, header::PingPayload};

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
    /// Per-stream window autotune gain: the window targets
    /// `gain x rate x RTT`.
    pub stream_window_gain: f64,
    /// Per-stream window growth limit: the window can at most multiply
    /// by this factor in a single update round.
    pub stream_window_growth: u32,
}

impl Default for GlobalPool {
    fn default() -> Self {
        Self {
            rtt: None,
            available: 0,
            probe: Probe::new(crate::config::DEFAULT_PING_INTERVAL, RammuxRole::Client),
            transit_send: None,
            transit_recv: None,
            stream_window_gain: 1.5,
            stream_window_growth: 2,
        }
    }
}

impl GlobalPool {
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
        let elapsed = recv.last_update.elapsed();
        let optimal = match self.rtt {
            Some(rtt) if recv.rate_ema > 0.0 && elapsed < 2 * rtt => {
                #[allow(clippy::cast_possible_truncation)]
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
    /// The clean RTT sample.
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
    /// Creates a new probe machine. The first probe is due after one
    /// interval: probing at t=0 would bury our own `CLEAR_LINK` behind
    /// the application's opening burst, so the exchange would cost a
    /// full transport-buffer drain instead of ~1.5 RTT.
    pub fn new(interval: Duration, role: RammuxRole) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(interval)),
            interval,
            started_at: Instant::now(),
            state: ProbeState::Idle,
            pong: None,
            role,
        }
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

    /// Drives the probe clock: schedules the next probe when due, and
    /// fails when the running probe does not complete within one interval.
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        std::task::ready!(self.sleep.poll_unpin(cx));
        match self.state {
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

    /// Enters a probe: data pauses now, `CLEAR_LINK` goes out next.
    fn begin(&mut self, syn: bool, next: AfterClear) {
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
        match (self.state, syn) {
            // The peer initiated: pause data, answer with our receipt,
            // then wait for the peer's probe ping.
            (ProbeState::Idle, true) => {
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
            // The peer's receipt: both directions are drained, measure.
            (ProbeState::AwaitClear { .. }, false) => {
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
        if self.pong.is_some() {
            return Err(ErrorKind::UnexpectedPing(payload));
        }
        match self.state {
            // The peer's probe ping arrived over the drained link: pong
            // it, send our own probe ping, and resume data right after.
            ProbeState::AwaitPing => {
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

    /// Next probe frame to send, if any, and whether sending it resumes
    /// data flow (the caller should wake the streams).
    pub fn next_frame(&mut self) -> Option<(ProbeFrame, bool)> {
        if let Some(payload) = self.pong.take() {
            return Some((ProbeFrame::Pong(payload), false));
        }
        match self.state {
            ProbeState::SendClear { syn, next } => {
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
        assert!(
            matches!(poll_probe(probe), Poll::Pending),
            "first probe is deferred"
        );
        tokio::time::advance(probe.interval).await;
        assert!(matches!(poll_probe(probe), Poll::Ready(Ok(()))));
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
        // No bare pings, and no receipt without an initiation.
        assert!(probe.on_ping(PingPayload::random()).is_err());
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
