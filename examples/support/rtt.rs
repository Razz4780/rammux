//! Scheduling rammux's RTT measurement.
//!
//! A [`RammuxConnection`] runs no timers: the link-clearing probe and the
//! plain ping are started by the application, and the connection never
//! gives up on one by itself. This module is the other half of that
//! arrangement - the policy the library used to hard-code, written once
//! as an application would write it.
//!
//! The schedule here reproduces rammux's historical built-in behaviour:
//!
//! - a link-clearing probe every 20s (+-10% jitter), re-anchored whenever
//!   one completes - including one the *peer* initiated, so two peers do
//!   not run probes back to back;
//! - a plain ping every 5s, giving up on one whose pong does not arrive
//!   within the same 5s;
//! - a probe that does not complete within one probe interval fails the
//!   connection, which is what makes it the liveness check.
//!
//! Drive it alongside the connection:
//!
//! ```ignore
//! let mut rtt = RttSchedule::new(probe_every, ping_every);
//! loop {
//!     tokio::select! {
//!         progress = conn.progress() => match progress? {
//!             RammuxProgress::Probe(event) => rtt.observe(event),
//!             // ...
//!         },
//!         due = rtt.next() => rtt.apply(due?, &mut conn)?,
//!     }
//! }
//! ```

#![allow(dead_code)]

use std::{fmt, time::Duration};

use rammux::{
    RammuxError,
    connection::{ProbeEvent, RammuxConnection},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::Instant,
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
        }
    }

    /// Sleeps until something is due.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancel safe: all of its state lives in `self`, so a
    /// cancelled call loses nothing but the sleep.
    pub async fn next(&mut self) -> Result<Due, ProbeTimeout> {
        loop {
            let deadlines = [
                Some(self.next_probe),
                Some(self.next_ping),
                self.probe_started.map(|at| at + self.probe_every),
                self.ping_sent.map(|at| at + self.ping_every),
            ];
            let at = deadlines.into_iter().flatten().min().expect("never empty");
            tokio::time::sleep_until(at).await;

            let now = Instant::now();
            if let Some(started) = self
                .probe_started
                .filter(|at| *at + self.probe_every <= now)
            {
                return Err(ProbeTimeout {
                    elapsed: now - started,
                });
            }
            if self.ping_sent.is_some_and(|at| at + self.ping_every <= now) {
                self.ping_sent = None;
                return Ok(Due::AbandonPing);
            }
            if self.next_probe <= now {
                return Ok(Due::Probe);
            }
            if self.next_ping <= now {
                return Ok(Due::Ping);
            }
        }
    }

    /// Carries out what [`Self::next`] returned.
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
