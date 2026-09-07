//! The loaded-RTT ping.
//!
//! rammux's one `PING` mechanism travels inline with data and times the round
//! trip through the queues that are actually standing: the transit layer's
//! credit wait, the socket buffers, the path. That is the yardstick stream
//! receive windows size from, because a stream's credit loop runs through
//! those same queues. The *clean* round trip, measured over a drained link,
//! is the transit layer's business and is reported through its statistics.
//!
//! Nothing here runs on a clock. A ping is started by the caller, nothing in
//! this crate gives up on it, and every transition is announced as a
//! [`PingEvent`] so the caller can impose whatever schedule and deadline it
//! wants. See [`RammuxConnection`](crate::connection::RammuxConnection) for
//! the caller-facing API.

use std::{collections::VecDeque, ops::Not, time::Duration};

use tokio::time::Instant;

use crate::{error::ErrorKind, header::PingPayload};

/// A transition of a connection's own `PING` exchange.
///
/// rammux runs no timers: a ping is started by the caller and nothing in
/// this crate ever gives up on one. These events are what an observer needs
/// to do that itself. An exchange is announced when it starts
/// ([`PingEvent::Sent`]) and again when it ends ([`PingEvent::Answered`] or
/// [`PingEvent::Abandoned`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PingEvent {
    /// A `PING` requested with
    /// [`RammuxConnection::send_ping`](crate::connection::RammuxConnection::send_ping)
    /// has been encoded, and its round trip is now being timed.
    Sent,
    /// The peer answered the `PING`.
    Answered {
        /// The loaded round trip: the path plus both sides' standing queues.
        rtt: Duration,
    },
    /// The `PING` was given up on with
    /// [`RammuxConnection::abandon_ping`](crate::connection::RammuxConnection::abandon_ping).
    /// A pong that arrives for it afterwards is ignored.
    Abandoned,
}

/// A `PING` frame ready to be encoded.
pub enum PingFrame {
    /// Our request.
    Ping(PingPayload),
    /// Response to the peer's request.
    Pong(PingPayload),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
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

/// The connection's `PING` exchanges: the one we run, and the answers we owe.
pub struct Ping {
    state: State,
    /// A payload we stopped waiting for. Its pong may still arrive, and is
    /// then ignored rather than failing the connection.
    forgotten: Option<PingPayload>,
    /// Pongs owed to the peer's pings. The peer keeps one ping in flight, so
    /// this holds one entry in practice.
    pongs: VecDeque<PingPayload>,
    /// Transitions not yet handed to the caller.
    events: VecDeque<PingEvent>,
}

impl Ping {
    /// Creates an idle ping machine.
    ///
    /// It sends nothing until [`Ping::send`] is called, and answers the
    /// peer's pings whenever they arrive.
    pub fn new() -> Self {
        Self {
            state: State::Idle,
            forgotten: None,
            pongs: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    /// Queues a ping.
    ///
    /// Returns `false` if one is already queued or in flight: only one ping
    /// is ever outstanding, so its pong is unambiguous.
    pub fn send(&mut self) -> bool {
        if matches!(self.state, State::Idle).not() {
            return false;
        }
        self.state = State::Queued {
            payload: PingPayload::random(),
        };
        true
    }

    /// Gives up on the outstanding ping.
    ///
    /// Returns whether one was actually in flight. A queued ping is dropped
    /// either way, but it never reached the wire, so there is nothing to
    /// announce and nothing to ignore later.
    pub fn abandon(&mut self) -> bool {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Awaiting { payload, .. } => {
                self.forgotten = Some(payload);
                self.events.push_back(PingEvent::Abandoned);
                true
            },
            State::Idle | State::Queued { .. } => false,
        }
    }

    /// Takes the next transition to report to the caller.
    pub fn next_event(&mut self) -> Option<PingEvent> {
        self.events.pop_front()
    }

    /// Handles the peer's `PING` request: a pong is owed.
    pub fn on_ping(&mut self, payload: PingPayload) {
        self.pongs.push_back(payload);
    }

    /// Handles an inbound `PONG`.
    ///
    /// Returns the round trip if it answered the outstanding ping. A pong
    /// for a ping we gave up on is ignored, and a pong for nothing we sent
    /// is a protocol violation.
    pub fn on_pong(&mut self, payload: PingPayload) -> Result<Option<Duration>, ErrorKind> {
        if let State::Awaiting {
            payload: expected,
            sent_at,
        } = self.state
            && expected == payload
        {
            self.state = State::Idle;
            let rtt = sent_at.elapsed();
            self.events.push_back(PingEvent::Answered { rtt });
            return Ok(Some(rtt));
        }
        if self.forgotten == Some(payload) {
            self.forgotten = None;
            return Ok(None);
        }
        Err(ErrorKind::UnexpectedPing(payload))
    }

    /// Next frame to send, if any.
    ///
    /// Owed pongs go first: the peer is timing them.
    pub fn next_frame(&mut self) -> Option<PingFrame> {
        if let Some(payload) = self.pongs.pop_front() {
            return Some(PingFrame::Pong(payload));
        }
        let State::Queued { payload } = self.state else {
            return None;
        };
        self.state = State::Awaiting {
            payload,
            sent_at: Instant::now(),
        };
        self.events.push_back(PingEvent::Sent);
        Some(PingFrame::Ping(payload))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Compact rendering of [`Ping::next_frame`].
    fn frame_kind(frame: Option<PingFrame>) -> Option<&'static str> {
        frame.map(|frame| match frame {
            PingFrame::Ping(..) => "ping",
            PingFrame::Pong(..) => "pong",
        })
    }

    fn events(ping: &mut Ping) -> Vec<PingEvent> {
        std::iter::from_fn(|| ping.next_event()).collect()
    }

    /// The machine has no clock of its own, so an unpolled one never sends
    /// anything.
    #[test]
    fn starts_idle_and_silent() {
        let mut ping = Ping::new();
        assert!(frame_kind(ping.next_frame()).is_none());
        assert!(events(&mut ping).is_empty());
    }

    /// A ping is a self-contained exchange: it goes out and comes back with
    /// a loaded sample.
    #[tokio::test(start_paused = true)]
    async fn a_ping_measures_the_loaded_rtt() {
        let mut ping = Ping::new();
        assert!(ping.send());
        assert!(events(&mut ping).is_empty(), "nothing has left yet");
        let Some(PingFrame::Ping(payload)) = ping.next_frame() else {
            panic!("expected a ping");
        };
        assert_eq!(events(&mut ping), [PingEvent::Sent]);
        assert!(!ping.send(), "only one ping is outstanding at a time");

        tokio::time::advance(Duration::from_millis(80)).await;
        let rtt = Duration::from_millis(80);
        assert_eq!(ping.on_pong(payload).unwrap(), Some(rtt));
        assert_eq!(events(&mut ping), [PingEvent::Answered { rtt }]);
        assert!(ping.send(), "the next one is free to go");
    }

    /// The peer's ping is answered, and answering is not a transition.
    #[test]
    fn the_peers_ping_is_answered() {
        let mut ping = Ping::new();
        ping.on_ping(PingPayload::random());
        assert_eq!(frame_kind(ping.next_frame()), Some("pong"));
        assert!(frame_kind(ping.next_frame()).is_none());
        assert!(events(&mut ping).is_empty());
    }

    /// The peer is timing its ping, so the pong we owe leads our own ping.
    #[test]
    fn owed_pongs_lead_our_ping() {
        let mut ping = Ping::new();
        assert!(ping.send());
        ping.on_ping(PingPayload::random());
        assert_eq!(frame_kind(ping.next_frame()), Some("pong"));
        assert_eq!(frame_kind(ping.next_frame()), Some("ping"));
        assert!(frame_kind(ping.next_frame()).is_none());
    }

    /// Giving up on a ping frees the next one, and the pong that eventually
    /// answers the abandoned one is ignored rather than failing the
    /// connection.
    #[test]
    fn abandoning_a_ping_frees_the_next_and_forgives_the_late_pong() {
        let mut ping = Ping::new();
        assert!(ping.send());
        let Some(PingFrame::Ping(abandoned)) = ping.next_frame() else {
            panic!("expected a ping");
        };
        assert_eq!(events(&mut ping), [PingEvent::Sent]);

        assert!(ping.abandon());
        assert!(!ping.abandon(), "nothing left to give up on");
        assert_eq!(events(&mut ping), [PingEvent::Abandoned]);

        assert!(ping.send());
        assert_eq!(
            ping.on_pong(abandoned).unwrap(),
            None,
            "a forgotten ping must not produce a sample"
        );
        assert!(events(&mut ping).is_empty());
    }

    /// A queued ping that never reached the wire is dropped silently: there
    /// is nothing outstanding to announce or to ignore later.
    #[test]
    fn abandoning_a_queued_ping_is_silent() {
        let mut ping = Ping::new();
        assert!(ping.send());
        assert!(!ping.abandon());
        assert!(events(&mut ping).is_empty());
        assert!(frame_kind(ping.next_frame()).is_none(), "ping was dropped");
    }

    /// A pong answering nothing we sent is a violation.
    #[test]
    fn a_pong_for_nothing_is_a_violation() {
        let mut ping = Ping::new();
        assert!(ping.on_pong(PingPayload::random()).is_err());

        assert!(ping.send());
        ping.next_frame();
        assert!(
            ping.on_pong(PingPayload::random()).is_err(),
            "a pong with the wrong payload answers nothing"
        );
    }
}
