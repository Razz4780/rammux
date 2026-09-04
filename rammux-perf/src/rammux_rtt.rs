//! Leftover from previous agent work

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use rammux::{
    RammuxError,
    connection::{ProbeEvent, RammuxConnection},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, Sleep},
};

/// What the schedule wants done next.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Due {
    /// Initiate a link-clearing probe.
    Probe,
    /// Send a plain ping.
    Ping,
    /// Give up on a plain ping whose pong never arrived. A lost pong is
    /// not fatal - unlike a lost probe, it holds nothing back but the
    /// next probe.
    AbandonPing,
}

/// A link-clearing probe passed its deadline: the peer is not answering.
///
/// The probe pauses data output on both sides, so an unfinished one stalls
/// the connection indefinitely. This is the connection's liveness check.
#[derive(Clone, Copy, Debug)]
pub struct ProbeTimeout {
    /// Time elapsed since the probe started.
    pub elapsed: Duration,
}

impl fmt::Display for ProbeTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "link-clearing probe timed out after {:.02}s",
            self.elapsed.as_secs_f32()
        )
    }
}

impl std::error::Error for ProbeTimeout {}

/// Schedules a connection's probes and pings, and times them out.
pub struct RttSchedule {
    probe_every: Duration,
    ping_every: Duration,
    /// When to next attempt a probe.
    next_probe: Instant,
    /// When to next send a plain ping.
    next_ping: Instant,
    /// When the running probe started, if one is running.
    probe_started: Option<Instant>,
    /// When the outstanding plain ping was sent, if one is outstanding.
    ping_sent: Option<Instant>,
    /// Timer, armed for [`Self::deadline`].
    sleep: Pin<Box<Sleep>>,
}

impl RttSchedule {
    /// Creates a schedule that probes every `probe_every` and pings every
    /// `ping_every`. Each interval doubles as that exchange's deadline.
    ///
    /// The first probe is due immediately: at connection open the link is
    /// empty by construction, which is the cheapest moment to clear it,
    /// and the session-level transit window cannot autotune until a clean
    /// RTT has landed.
    pub fn new(probe_every: Duration, ping_every: Duration) -> Self {
        let now = Instant::now();
        Self {
            probe_every,
            ping_every,
            next_probe: now,
            next_ping: now + ping_every,
            probe_started: None,
            ping_sent: None,
            sleep: Box::pin(tokio::time::sleep_until(now)),
        }
    }

    /// When the next thing is due.
    ///
    /// Whichever deadline this picks, its check in [`Self::poll_next`] fires
    /// once the timer reaches it, so a poll never comes back empty-handed.
    fn deadline(&self) -> Instant {
        [
            Some(self.next_probe),
            Some(self.next_ping),
            self.probe_started.map(|at| at + self.probe_every),
            self.ping_sent.map(|at| at + self.ping_every),
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("never empty")
    }

    /// Polls until something is due.
    ///
    /// All of the state lives in `self`, so a caller that stops polling
    /// loses nothing but the timer registration.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Due, ProbeTimeout>> {
        let at = self.deadline();
        if self.sleep.deadline() != at {
            self.sleep.as_mut().reset(at);
        }
        std::task::ready!(self.sleep.as_mut().poll(cx));

        let now = Instant::now();
        if let Some(started) = self
            .probe_started
            .filter(|at| *at + self.probe_every <= now)
        {
            return Poll::Ready(Err(ProbeTimeout {
                elapsed: now - started,
            }));
        }
        if self.ping_sent.is_some_and(|at| at + self.ping_every <= now) {
            self.ping_sent = None;
            return Poll::Ready(Ok(Due::AbandonPing));
        }
        if self.next_probe <= now {
            return Poll::Ready(Ok(Due::Probe));
        }
        Poll::Ready(Ok(Due::Ping))
    }

    /// Carries out what [`Self::poll_next`] returned.
    pub fn apply<IO>(
        &mut self,
        due: Due,
        conn: &mut RammuxConnection<IO>,
    ) -> Result<(), RammuxError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let now = Instant::now();
        match due {
            Due::Probe => {
                // Refused means a probe is already running - the peer's,
                // say - or a plain ping is unanswered and its pong would
                // be indistinguishable from the probe's own. Both resolve
                // on their own, so come back rather than force it.
                self.next_probe = now
                    + if conn.start_probe()? {
                        self.probe_every
                    } else {
                        self.ping_every
                    };
            },
            Due::Ping => {
                conn.send_ping()?;
                self.next_ping = now + self.ping_every;
            },
            Due::AbandonPing => {
                conn.abandon_ping()?;
            },
        }
        Ok(())
    }

    /// Folds a reported transition back into the schedule.
    pub fn observe(&mut self, event: ProbeEvent) {
        let now = Instant::now();
        match event {
            ProbeEvent::PingSent => self.ping_sent = Some(now),
            ProbeEvent::PingAnswered { .. } | ProbeEvent::PingAbandoned => {
                self.ping_sent = None;
                self.next_ping = now + self.ping_every;
            },
            ProbeEvent::ProbeStarted { .. } => self.probe_started = Some(now),
            ProbeEvent::ProbeCompleted { .. } => {
                self.probe_started = None;
                // Re-anchor on completion, jittered by +-10%. A probe the
                // peer initiated re-anchors us too, so the two sides do
                // not run one probe right after another; the jitter keeps
                // their clocks from staying phase-locked, which would
                // make colliding initiations the steady state.
                let jitter = 0.9 + 0.2 * rand::random::<f64>();
                self.next_probe = now + self.probe_every.mul_f64(jitter);
            },
            _ => {},
        }
    }
}
