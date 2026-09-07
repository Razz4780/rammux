//! The link-clearing probe: a round trip no queue can inflate.
//!
//! Every rule that sizes a window from `c x RTT x rate` needs the *clean*
//! round trip, not the loaded one. Sizing from the loaded one runs away: a
//! bigger window queues more, which raises the loaded round trip, which raises
//! the ceiling, which raises the window again.
//!
//! Draining the link is the only way to see the path's own cost, and it works
//! because the frames travel in the same ordered stream as the data. This end
//! pauses its data output, so `CLEAR_LINK` leaves behind everything already
//! queued and cannot arrive until the forward path has emptied. The peer pauses
//! in turn, so its `CLEAR_ACK` cannot arrive until the reverse path has emptied
//! too. Only then is the `PING` timed, over a link with nothing in it.
//!
//! It is not free - it costs about two round trips of paused output plus a
//! window's drain - so it is scheduled, and only for configurations that use
//! the answer. The schedule is LEDBAT++'s: the next exchange starts at a fixed
//! multiple of how long the last one took. An exchange's duration is its cost,
//! and a window that is large on a slow link makes it expensive, so spacing the
//! exchanges by their own duration holds the cost to a bounded share of the
//! connection's time on any link, where a fixed interval could only be tuned
//! to some of them. Control
//! frames are exempt from the pause: they are 8 bytes, and holding a credit
//! return for the length of a probe would stall the peer outright.
//!
//! Only one end probes. The round trip is a property of the path, so a second
//! measurement would pay the same cost for the same number; the initiator
//! reports what it measured in a `CLEAN_RTT` frame and both ends size from it.
//!
//! # Two ways an exchange can fail, and what happens
//!
//! *Both ends configured as [`Role::Initiator`].* Left alone, each would answer
//! the other's `CLEAR_LINK`, abandon its own exchange to do so, and wait for a
//! `PING` neither will send - output paused on both sides for good. The roles
//! are a configuration contract, so an initiator that receives a `CLEAR_LINK`
//! fails the connection with an error naming the problem, rather than hanging.
//!
//! *A peer that never answers.* Every exchange has a deadline
//! ([`EXCHANGE_DEADLINE`]); past it, this end resumes its output and tries
//! again at the usual interval. The connection degrades to sizing from its last
//! known round trip, which is a better outcome than either hanging or failing:
//! the data path may be perfectly healthy, and TCP will report a peer that is
//! actually gone. The deadline is generous because an exchange has to drain a
//! whole window before the `CLEAR_LINK` even arrives, and a window is allowed to
//! be large on a slow link.

use std::{future::Future, io, pin::Pin, task::Context, time::Duration};

use tokio::time::{Instant, Sleep, sleep_until};

use crate::frame::Frame;

/// How long an exchange may take before it is given up on.
///
/// Draining a 16 MiB window - the default cap - takes 13 seconds at 10 Mbit,
/// and the `CLEAR_LINK` sits behind it. Anything much slower than that is a
/// window that should never have grown so large on such a link.
pub const EXCHANGE_DEADLINE: Duration = Duration::from_secs(30);

/// Shortest gap between exchanges, whatever the multiplier says.
///
/// Only there to keep a path with a microsecond round trip - a test over an
/// in-memory pipe - from probing in a tight loop.
const MIN_SPACING: Duration = Duration::from_millis(100);

/// Longest gap between exchanges, whatever the multiplier says.
///
/// Half the delay base's memory. The probe is what re-establishes that base -
/// the first frames after a drain carry the true propagation delay - and a base
/// that outlives its last clean sample lets a standing queue become the base,
/// after which the window ratchets. This also covers an exchange that timed
/// out, whose duration is the deadline and would otherwise schedule the next
/// one minutes away.
const MAX_SPACING: Duration = Duration::from_secs(30);

/// Which end pays for the probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Schedules the exchanges, times the `PING`, and reports what it
    /// measured. Exactly one end of a connection must be this.
    Initiator,
    /// Answers exchanges the peer opens, and sizes from the round trip it
    /// reports.
    Responder,
}

#[derive(Debug, Clone, Copy)]
enum State {
    /// Nothing in flight; data output runs.
    Idle,
    /// We paused and sent `CLEAR_LINK`.
    AwaitingAck { seq: u32 },
    /// Both ends are paused and the link is drained. Timing the `PING`.
    AwaitingPong { seq: u32, sent_at: Instant },
    /// The peer is probing us. Paused until we answer its `PING`.
    Responding,
}

/// What the connection should do about a frame the probe just saw.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    /// A frame to put on the wire.
    pub reply: Option<Frame>,
    /// Whether an exchange just ended, so data output resumes and every clock
    /// the pause disturbed should restart.
    pub finished: bool,
}

pub(crate) struct Probe {
    state: State,
    seq: u32,
    /// While idle, fires when the next exchange is due. During an exchange,
    /// fires when it has taken too long - the same timer does both jobs
    /// because the two never overlap.
    timer: Pin<Box<Sleep>>,
    /// How many of the last exchange's durations to wait before the next.
    spacing: f64,
    /// When the exchange in progress began.
    paused_since: Instant,
    /// The gap scheduled after the last exchange, for the trace log.
    interval: Duration,
    role: Role,
    /// Whether anything in this configuration uses the answer.
    wanted: bool,
    clean_rtt: Option<Duration>,
    completed: u64,
    abandoned: u64,
}

impl Probe {
    pub(crate) fn new(role: Role, spacing: f64, wanted: bool) -> Self {
        Self {
            state: State::Idle,
            seq: 0,
            // The link is empty before any data flows, so the first exchange is
            // both the cheapest one and the one the connection's ramp needs.
            timer: Box::pin(sleep_until(Instant::now())),
            spacing,
            paused_since: Instant::now(),
            interval: Duration::ZERO,
            role,
            wanted,
            clean_rtt: None,
            completed: 0,
            abandoned: 0,
        }
    }

    /// Whether data output is held back for an exchange in progress.
    pub(crate) fn paused(&self) -> bool {
        !matches!(self.state, State::Idle)
    }

    pub(crate) fn clean_rtt(&self) -> Option<Duration> {
        self.clean_rtt
    }

    pub(crate) fn completed(&self) -> u64 {
        self.completed
    }

    /// Exchanges given up on for want of an answer.
    pub(crate) fn abandoned(&self) -> u64 {
        self.abandoned
    }

    /// The gap the schedule chose after the last exchange.
    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    /// Advances the clock: opens an exchange if one is due, returning the frame
    /// that starts it, or gives up on one that has overrun its deadline.
    ///
    /// Both ends call this, so a responder left waiting for a `PING` that never
    /// comes also finds its way out.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Option<Frame> {
        if self.paused() {
            if self.timer.as_mut().poll(cx).is_ready() {
                self.abandoned += 1;
                self.finish();
            }
            return None;
        }
        if self.role != Role::Initiator || !self.wanted || self.timer.as_mut().poll(cx).is_pending()
        {
            return None;
        }
        self.seq += 1;
        self.pause(State::AwaitingAck { seq: self.seq });
        Some(Frame::ClearLink(self.seq))
    }

    /// Feeds the state machine a frame that belongs to it.
    pub(crate) fn on_frame(&mut self, frame: Frame) -> io::Result<Outcome> {
        let outcome = match frame {
            // Only an initiator sends this, and there is meant to be one.
            Frame::ClearLink(..) if self.role == Role::Initiator => {
                return Err(io::Error::other(
                    "both ends of the connection are configured as the probe initiator",
                ));
            },
            // Pause and answer.
            Frame::ClearLink(seq) => {
                self.pause(State::Responding);
                Outcome {
                    reply: Some(Frame::ClearAck(seq)),
                    finished: false,
                }
            },
            // Both directions have drained. Time the ping across the gap.
            Frame::ClearAck(seq) if matches!(self.state, State::AwaitingAck { seq: ours } if ours == seq) =>
            {
                self.state = State::AwaitingPong {
                    seq,
                    sent_at: Instant::now(),
                };
                Outcome {
                    reply: Some(Frame::Ping(seq)),
                    finished: false,
                }
            },
            Frame::Ping(seq) if matches!(self.state, State::Responding) => {
                self.finish();
                Outcome {
                    reply: Some(Frame::Pong(seq)),
                    finished: true,
                }
            },
            Frame::Pong(seq) => {
                let State::AwaitingPong { seq: ours, sent_at } = self.state else {
                    return Ok(Outcome::default());
                };
                if ours != seq {
                    return Ok(Outcome::default());
                }
                let rtt = sent_at.elapsed();
                self.clean_rtt = Some(rtt);
                self.completed += 1;
                self.finish();
                let micros = u32::try_from(rtt.as_micros()).unwrap_or(u32::MAX);
                Outcome {
                    reply: Some(Frame::CleanRtt(micros)),
                    finished: true,
                }
            },
            // The other end measured it; no exchange of ours to end.
            Frame::CleanRtt(micros) => {
                self.clean_rtt = Some(Duration::from_micros(micros.into()));
                Outcome::default()
            },
            _ => Outcome::default(),
        };
        Ok(outcome)
    }

    /// Enters an exchange, and starts the clock it has to finish by.
    fn pause(&mut self, state: State) {
        self.state = state;
        self.paused_since = Instant::now();
        self.timer
            .as_mut()
            .reset(Instant::now() + EXCHANGE_DEADLINE);
    }

    /// Resumes data output and schedules the next exchange, at a multiple of
    /// how long this one took.
    fn finish(&mut self) {
        self.state = State::Idle;
        let took = self.paused_since.elapsed();
        self.interval = took.mul_f64(self.spacing).clamp(MIN_SPACING, MAX_SPACING);
        self.timer.as_mut().reset(Instant::now() + self.interval);
    }
}
