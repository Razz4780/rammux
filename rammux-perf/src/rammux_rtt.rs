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
    /// How long a probe gets before it is a [`ProbeTimeout`].
    ///
    /// Also the interval between our own probes, when we initiate any. It is
    /// kept even when we do not, because the *peer* can start a probe - that
    /// pauses this side's data too, so it still needs a deadline.
    probe_every: Duration,
    ping_every: Duration,
    /// When to next attempt a probe, or [`None`] on a connection that never
    /// initiates one.
    next_probe: Option<Instant>,
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
        Self::build(Some(Instant::now()), probe_every, ping_every)
    }

    /// Creates a schedule that only ever pings.
    ///
    /// For a connection whose transit window is disabled. The clean RTT a
    /// probe measures has exactly one consumer - the session-level transit
    /// window is sized from it - so with that window off the probe buys
    /// nothing, and it is not free: it pauses data output on *both* sides
    /// for a round trip. The plain ping is unaffected, and it is the one
    /// that matters here anyway, since per-stream receive windows are sized
    /// from the loaded RTT it measures.
    ///
    /// Nothing is lost with the probe. A probe is also how a dead peer is
    /// detected, but only because an unfinished one stalls the connection
    /// indefinitely - a hazard that exists because probes do. With none of
    /// our own to be stuck on, there is nothing to detect; `probe_timeout`
    /// stays as the deadline for a probe the peer initiates.
    pub fn pings_only(probe_timeout: Duration, ping_every: Duration) -> Self {
        Self::build(None, probe_timeout, ping_every)
    }

    fn build(next_probe: Option<Instant>, probe_every: Duration, ping_every: Duration) -> Self {
        let now = Instant::now();
        Self {
            probe_every,
            ping_every,
            next_probe,
            next_ping: now + ping_every,
            probe_started: None,
            ping_sent: None,
            sleep: Box::pin(tokio::time::sleep_until(next_probe.unwrap_or(now))),
        }
    }

    /// When the next thing is due.
    ///
    /// Whichever deadline this picks, its check in [`Self::poll_next`] fires
    /// once the timer reaches it, so a poll never comes back empty-handed.
    fn deadline(&self) -> Instant {
        [
            self.next_probe,
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
        if self.next_probe.is_some_and(|at| at <= now) {
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
                self.next_probe = Some(
                    now + if conn.start_probe()? {
                        self.probe_every
                    } else {
                        self.ping_every
                    },
                );
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
                //
                // A schedule that never initiates one stays that way: this
                // fires for the peer's probes as well as our own.
                let jitter = 0.9 + 0.2 * rand::random::<f64>();
                self.next_probe = self
                    .next_probe
                    .map(|_| now + self.probe_every.mul_f64(jitter));
            },
            _ => {},
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const PROBE: Duration = Duration::from_secs(10);
    const PING: Duration = Duration::from_secs(5);

    /// The clean RTT a probe measures is read by exactly one thing - the
    /// transit window sizes itself from it - so a connection with that window
    /// disabled has nothing to probe for, and probing anyway would stall data
    /// output on both sides for a round trip to produce a number nobody
    /// reads. Cheap to state, and invisible in a benchmark: the run still
    /// works, it just carries a periodic stall in its latency tail.
    #[tokio::test(start_paused = true)]
    async fn a_schedule_without_probes_only_ever_pings() {
        let mut schedule = RttSchedule::pings_only(PROBE, PING);
        for _ in 0..10 {
            let due = std::future::poll_fn(|cx| schedule.poll_next(cx))
                .await
                .expect("nothing can time out with no probe running");
            assert_eq!(due, Due::Ping);
            // Stands in for `apply`, which needs a connection to send on.
            schedule.next_ping = Instant::now() + PING;
        }
    }

    /// The other half of the same statement: a connection that does autotune
    /// probes at once, because the window cannot move until a clean RTT has
    /// landed and an idle link is the cheapest moment to clear.
    #[tokio::test(start_paused = true)]
    async fn a_schedule_with_probes_opens_with_one() {
        let mut schedule = RttSchedule::new(PROBE, PING);
        let due = std::future::poll_fn(|cx| schedule.poll_next(cx))
            .await
            .unwrap();
        assert_eq!(due, Due::Probe);
    }

    /// A probe the peer starts still has to be timed out, whether or not this
    /// side initiates any: it pauses this side's data output just the same,
    /// and nothing in rammux gives up on it.
    #[tokio::test(start_paused = true)]
    async fn a_peer_probe_is_still_timed_out_without_probing() {
        let mut schedule = RttSchedule::pings_only(PROBE, PING);
        schedule.observe(ProbeEvent::ProbeStarted { initiated: false });
        let timeout = loop {
            match std::future::poll_fn(|cx| schedule.poll_next(cx)).await {
                Ok(Due::Ping) => schedule.next_ping = Instant::now() + PING,
                Ok(due) => panic!("{due:?} on a schedule that does not probe"),
                Err(timeout) => break timeout,
            }
        };
        assert!(timeout.elapsed >= PROBE);
    }
}
