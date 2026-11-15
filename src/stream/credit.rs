//! EXPERIMENT: when the receiver returns credit. Not part of the crate proper.

use std::{cell::Cell, time::Duration};

use tokio::time::Instant;

use crate::stream::StreamRef;

/// When a stream's pending credit is sent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CreditPolicy {
    /// Today: every `T_c` bytes, each stream alone.
    Fixed,
    /// Due at `T_c`, then wait `srtt / 8` and flush everything (trailing edge).
    Delay,
    /// Due at `T_c`. A flush takes a token: `burst` tokens, one more every `srtt / 8`.
    Gate { burst: u32 },
    /// Due at `max(T_c, V / div)`, `V` being what the stream consumed over the last `srtt`.
    Proportional { div: f64 },
}

#[derive(Clone, Copy, Debug)]
pub struct Experiment {
    pub policy: CreditPolicy,
    /// At most one `PING` per `srtt / 8`, and credit frames carry one when the gap allows.
    pub new_pings: bool,
    /// A `PONG` that goes out takes the pending credit along.
    pub pong_riders: bool,
}

thread_local! {
    pub static EXPERIMENT: Cell<Experiment> = Cell::new(match std::env::var("CREDIT_POLICY").as_deref() {
        Ok("prop8") => Experiment { policy: CreditPolicy::Proportional { div: 8.0 }, new_pings: true, pong_riders: true },
        Ok("gate4") => Experiment { policy: CreditPolicy::Gate { burst: 4 }, new_pings: true, pong_riders: true },
        _ => Experiment { policy: CreditPolicy::Fixed, new_pings: false, pong_riders: false },
    });
}

pub struct CreditGate {
    pub policy: CreditPolicy,
    pong_riders: bool,
    /// A flush is going out: every stream with credit above the floor adds its own.
    pub flushing: bool,
    /// Streams with credit above the floor that was not sent.
    waiters: Vec<StreamRef>,
    tokens: f64,
    refilled_at: Option<Instant>,
    /// When the pending credit goes out at the latest.
    pub deadline: Option<Instant>,
}

impl CreditGate {
    pub fn new() -> Self {
        Self {
            policy: EXPERIMENT.get().policy,
            pong_riders: EXPERIMENT.get().pong_riders,
            flushing: false,
            waiters: Vec::new(),
            tokens: 0.0,
            refilled_at: None,
            deadline: None,
        }
    }

    pub fn riders(&self) -> bool {
        self.policy != CreditPolicy::Fixed
    }

    pub fn pong_riders(&self) -> bool {
        self.pong_riders && self.riders()
    }

    pub fn open_flush(&mut self) {
        self.flushing = true;
        self.deadline = None;
        for waiter in self.waiters.drain(..) {
            waiter.updates.wake();
        }
    }

    pub fn close_flush(&mut self) {
        self.flushing = false;
    }

    pub fn wait(&mut self, stream: &StreamRef) {
        self.waiters.push(stream.clone());
    }

    fn refill(&mut self, now: Instant, srtt: Duration, burst: u32) -> f64 {
        let interval = srtt.as_secs_f64() / 8.0;
        let elapsed = self
            .refilled_at
            .map_or(f64::INFINITY, |at| (now - at).as_secs_f64());
        self.tokens = (self.tokens + elapsed / interval).min(f64::from(burst));
        self.refilled_at = Some(now);
        interval
    }

    /// A stream whose credit is due asks to send it.
    pub fn request(&mut self, now: Instant, srtt: Option<Duration>) -> bool {
        match self.policy {
            CreditPolicy::Fixed => true,
            CreditPolicy::Proportional { .. } => {
                self.open_flush();
                true
            },
            CreditPolicy::Gate { burst } => {
                let Some(srtt) = srtt else {
                    self.open_flush();
                    return true;
                };
                let interval = self.refill(now, srtt, burst);
                if self.tokens >= 1.0 {
                    self.tokens -= 1.0;
                    self.open_flush();
                    true
                } else {
                    let wait = Duration::from_secs_f64((1.0 - self.tokens) * interval);
                    self.deadline.get_or_insert(now + wait);
                    false
                }
            },
            CreditPolicy::Delay => {
                let Some(srtt) = srtt else {
                    return true;
                };
                self.deadline.get_or_insert(now + srtt / 8);
                false
            },
        }
    }

    /// Credit of at least `T_c` stays behind.
    pub fn tail(&mut self, now: Instant, srtt: Option<Duration>) {
        if let (CreditPolicy::Proportional { .. }, Some(srtt)) = (self.policy, srtt) {
            self.deadline.get_or_insert(now + srtt / 4);
        }
    }

    pub fn on_poll(&mut self, now: Instant, srtt: Option<Duration>) {
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            if let (CreditPolicy::Gate { burst }, Some(srtt)) = (self.policy, srtt) {
                self.refill(now, srtt, burst);
                self.tokens = (self.tokens - 1.0).max(0.0);
            }
            self.open_flush();
        }
    }
}
