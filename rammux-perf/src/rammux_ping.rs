//! The ping schedule a rammux connection needs driven, and the deadline that
//! makes it a liveness check.

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use rammux::{
    RammuxError,
    connection::{PingEvent, RammuxConnection},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, Sleep},
};

/// A `PING` passed its deadline: the peer is not answering.
///
/// rammux gives up on nothing by itself, and the transit layer underneath
/// degrades rather than fails when its probe goes unanswered, so this is the
/// connection's one liveness check.
#[derive(Clone, Copy, Debug)]
pub struct PingTimeout {
    /// Time elapsed since the ping was sent.
    pub elapsed: Duration,
}

impl fmt::Display for PingTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rammux ping timed out after {:.02}s",
            self.elapsed.as_secs_f32()
        )
    }
}

impl std::error::Error for PingTimeout {}

/// Sends a connection's pings on an interval, and times them out.
pub struct PingSchedule {
    every: Duration,
    timeout: Duration,
    /// When to next send a ping.
    next: Instant,
    /// When the outstanding ping was sent, if one is outstanding.
    sent: Option<Instant>,
    /// Timer, armed for [`Self::deadline`].
    sleep: Pin<Box<Sleep>>,
}

impl PingSchedule {
    /// Creates a schedule that pings every `every` and declares the peer
    /// dead once a ping has gone `timeout` without an answer.
    ///
    /// The first ping goes out after one interval: at connection open there
    /// is nothing loaded to measure yet.
    pub fn new(every: Duration, timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            every,
            timeout,
            next: now + every,
            sent: None,
            sleep: Box::pin(tokio::time::sleep_until(now)),
        }
    }

    /// When the next thing is due: the next ping, or the outstanding one's
    /// deadline, whichever comes first.
    fn deadline(&self) -> Instant {
        self.sent
            .map_or(self.next, |at| (at + self.timeout).min(self.next))
    }

    /// Polls until a ping is due, or the outstanding one has run out of time.
    ///
    /// All of the state lives in `self`, so a caller that stops polling
    /// loses nothing but the timer registration.
    pub fn poll_due(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), PingTimeout>> {
        let at = self.deadline();
        if self.sleep.deadline() != at {
            self.sleep.as_mut().reset(at);
        }
        std::task::ready!(self.sleep.as_mut().poll(cx));

        let now = Instant::now();
        if let Some(sent) = self.sent.filter(|at| *at + self.timeout <= now) {
            return Poll::Ready(Err(PingTimeout {
                elapsed: now - sent,
            }));
        }
        Poll::Ready(Ok(()))
    }

    /// Sends the ping that is due.
    ///
    /// A refusal - the previous ping is still outstanding - means the
    /// interval is shorter than the round trip through the connection's
    /// queues. The outstanding ping keeps its deadline, and the next attempt
    /// is an interval away either way.
    pub fn ping<IO>(&mut self, conn: &mut RammuxConnection<IO>) -> Result<(), RammuxError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        conn.send_ping()?;
        self.next = Instant::now() + self.every;
        Ok(())
    }

    /// Folds a reported transition back into the schedule.
    pub fn observe(&mut self, event: PingEvent) {
        match event {
            PingEvent::Sent => self.sent = Some(Instant::now()),
            PingEvent::Answered { .. } | PingEvent::Abandoned => self.sent = None,
            _ => {},
        }
    }
}
