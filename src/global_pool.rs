use std::{
    num::NonZeroU32,
    ops::Not,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::{Instant, Sleep};

use crate::{error::ErrorKind, header::PingPayload};

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
            probe: Probe::new(crate::config::DEFAULT_PING_INTERVAL),
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
/// transport is ordered, so a `CLEAR_LINK` frame is a drain barrier):
///
/// ```text
/// I: pause data, send CLEAR_LINK
/// R: receive CLEAR_LINK -> pause data, send CLEAR_LINK
/// I: receive CLEAR_LINK (R->I is drained) -> send PING
/// R: receive PING (I->R is drained) -> send PONG, own PING, resume data
/// I: receive PONG (clean RTT), receive R's PING -> send PONG, resume data
/// R: receive PONG (clean RTT)
/// ```
///
/// Both sides can initiate simultaneously; each then treats the peer's
/// `CLEAR_LINK` as the response to its own and both act as initiators.
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
}

/// See [`Probe`] doc for the dance these states walk through.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ProbeState {
    /// No probe in flight; data flows.
    Idle,
    /// Data paused; our `CLEAR_LINK` waits to be sent, then `next`.
    SendClear { next: AfterClear },
    /// Initiator: `CLEAR_LINK` sent, waiting for the peer's.
    AwaitClear,
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
    /// We initiated: wait for the peer's `CLEAR_LINK`.
    AwaitClear,
    /// The peer initiated: wait for the peer's probe `PING`.
    AwaitPing,
    /// Collision - the peer's `CLEAR_LINK` already arrived: follow our
    /// `CLEAR_LINK` directly with the probe `PING`.
    SendPing,
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
    /// `CLEAR_LINK`.
    Clear,
    /// Our probe `PING` request.
    Ping(PingPayload),
    /// Response to the peer's probe `PING`.
    Pong(PingPayload),
}

impl Probe {
    /// Creates a new probe machine. The first probe is due immediately.
    pub fn new(interval: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            started_at: Instant::now(),
            state: ProbeState::Idle,
            pong: None,
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
            ProbeState::Idle | ProbeState::AwaitClear => false,
            ProbeState::SendClear { next } => next != AfterClear::AwaitClear,
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
                self.begin(AfterClear::AwaitClear);
                Poll::Ready(Ok(()))
            },
            _ => Poll::Ready(Err(ErrorKind::ProbeTimeout {
                elapsed: self.started_at.elapsed(),
            })),
        }
    }

    /// Enters a probe: data pauses now, `CLEAR_LINK` goes out next.
    fn begin(&mut self, next: AfterClear) {
        self.started_at = Instant::now();
        self.sleep.as_mut().reset(self.started_at + self.interval);
        self.state = ProbeState::SendClear { next };
    }

    /// Completes a probe and schedules the next one.
    fn finish(&mut self, rtt: Duration, resumed: bool) -> Option<ProbeDone> {
        self.state = ProbeState::Idle;
        self.sleep.as_mut().reset(Instant::now() + self.interval);
        Some(ProbeDone {
            rtt,
            resume: !resumed,
        })
    }

    /// Handles an inbound `CLEAR_LINK`.
    pub fn on_clear_link(&mut self) -> Result<(), ErrorKind> {
        match self.state {
            // The peer initiated: pause data, answer with our own
            // CLEAR_LINK, then wait for the peer's probe ping.
            ProbeState::Idle => {
                self.begin(AfterClear::AwaitPing);
                Ok(())
            },
            // Collision before our CLEAR_LINK went out: the peer's counts
            // as the response to ours, so ours is followed directly by the
            // probe ping.
            ProbeState::SendClear {
                next: AfterClear::AwaitClear,
            } => {
                self.state = ProbeState::SendClear {
                    next: AfterClear::SendPing,
                };
                Ok(())
            },
            // Our own probe: the peer's response (or a colliding
            // initiation - same thing). The link toward us is drained;
            // measure it.
            ProbeState::AwaitClear => {
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
            ProbeState::SendPing {
                resume,
                peer_pinged: false,
            } => {
                self.pong = Some(payload);
                self.state = ProbeState::SendPing {
                    resume,
                    peer_pinged: true,
                };
                Ok(None)
            },
            ProbeState::AwaitPong {
                payload: ours,
                sent_at,
                resumed,
                peer_pinged: false,
                rtt,
            } => {
                self.pong = Some(payload);
                match rtt {
                    Some(rtt) => Ok(self.finish(rtt, resumed)),
                    None => {
                        self.state = ProbeState::AwaitPong {
                            payload: ours,
                            sent_at,
                            resumed,
                            peer_pinged: true,
                            rtt,
                        };
                        Ok(None)
                    },
                }
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
            ProbeState::SendClear { next } => {
                self.state = match next {
                    AfterClear::AwaitClear => ProbeState::AwaitClear,
                    AfterClear::AwaitPing => ProbeState::AwaitPing,
                    AfterClear::SendPing => ProbeState::SendPing {
                        resume: false,
                        peer_pinged: false,
                    },
                };
                Some((ProbeFrame::Clear, false))
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

    fn poll_probe(probe: &mut Probe) -> Poll<Result<(), ErrorKind>> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        probe.poll(&mut cx)
    }

    fn frame_kind(frame: Option<(ProbeFrame, bool)>) -> Option<(&'static str, bool)> {
        frame.map(|(frame, resume)| {
            let kind = match frame {
                ProbeFrame::Clear => "clear",
                ProbeFrame::Ping(..) => "ping",
                ProbeFrame::Pong(..) => "pong",
            };
            (kind, resume)
        })
    }

    #[tokio::test(start_paused = true)]
    async fn initiator_flow() {
        let mut probe = Probe::new(Duration::from_secs(5));
        // Due immediately.
        assert!(matches!(poll_probe(&mut probe), Poll::Ready(Ok(()))));
        assert!(probe.paused());
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        assert!(frame_kind(probe.next_frame()).is_none());
        // Peer's CLEAR_LINK arrives: our ping goes out.
        probe.on_clear_link().unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", false)));
        assert!(probe.paused());
        let ProbeState::AwaitPong { payload, .. } = probe.state else {
            panic!("expected AwaitPong");
        };
        // Pong arrives first, then the peer's own ping (asymmetric order).
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
        let mut probe = Probe::new(Duration::from_secs(5));
        probe
            .sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_secs(5));
        // Peer initiates.
        probe.on_clear_link().unwrap();
        assert!(probe.paused());
        assert!(probe.peer_must_be_silent());
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        // Peer's probe ping arrives over the drained link.
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
    async fn collision_flow() {
        let mut probe = Probe::new(Duration::from_secs(5));
        assert!(matches!(poll_probe(&mut probe), Poll::Ready(Ok(()))));
        // The peer's CLEAR_LINK arrives before ours went out.
        probe.on_clear_link().unwrap();
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        assert_eq!(frame_kind(probe.next_frame()), Some(("ping", false)));
        let ProbeState::AwaitPong { payload, .. } = probe.state else {
            panic!("expected AwaitPong");
        };
        // Symmetric order: the peer's ping first, then its pong.
        assert!(probe.on_ping(PingPayload::random()).unwrap().is_none());
        assert_eq!(frame_kind(probe.next_frame()), Some(("pong", false)));
        let done = probe.on_pong(payload).unwrap().unwrap();
        assert!(done.resume);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_kills() {
        let mut probe = Probe::new(Duration::from_secs(5));
        assert!(matches!(poll_probe(&mut probe), Poll::Ready(Ok(()))));
        assert_eq!(frame_kind(probe.next_frame()), Some(("clear", false)));
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(matches!(
            poll_probe(&mut probe),
            Poll::Ready(Err(ErrorKind::ProbeTimeout { .. }))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn strictness() {
        let mut probe = Probe::new(Duration::from_secs(5));
        probe
            .sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_secs(5));
        // No bare pings: a PING outside a probe is a violation.
        assert!(probe.on_ping(PingPayload::random()).is_err());
        assert!(probe.on_pong(PingPayload::random()).is_err());
        // Responder cannot receive a second CLEAR_LINK.
        probe.on_clear_link().unwrap();
        probe.next_frame();
        assert!(probe.on_clear_link().is_err());
    }
}
