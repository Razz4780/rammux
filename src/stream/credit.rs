//! EXPERIMENT: when the receiver returns credit. Not part of the crate proper.

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    time::Duration,
};

use tokio::time::Instant;

use crate::{StreamId, config::RammuxRole, stream::StreamRef};

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
    /// The sender marks the bytes after which it wants credit: every `max(T_c, unacked / div)` while it
    /// borrows, when its base window fills, and when it is blocked.
    Marks { div: f64 },
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
            CreditPolicy::Proportional { .. } | CreditPolicy::Marks { .. } => {
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
        if let (CreditPolicy::Proportional { .. } | CreditPolicy::Marks { .. }, Some(srtt)) =
            (self.policy, srtt)
        {
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

/// EXPERIMENT: the marks travel out of band. A direction of a stream is known by the role of its sender and the ID.
/// A mark is a byte offset: it counts once the receiver's application consumed that many bytes, so it never
/// acts before the marked data could have arrived.
pub type MarkKey = (RammuxRole, StreamId);

#[derive(Default)]
struct MarkChannel {
    marks: VecDeque<u64>,
    receiver: Option<StreamRef>,
}

thread_local! {
    /// EXPERIMENT: credit goes out before data.
    pub static CONTROL_FIRST: Cell<bool> = const { Cell::new(false) };
    static MARKS: RefCell<HashMap<MarkKey, MarkChannel>> = RefCell::default();
    /// The one-way delay of the path, which a mark without data (a request) takes.
    pub static ONE_WAY: Cell<Duration> = const { Cell::new(Duration::ZERO) };
    /// Marks and requests sent.
    pub static MARK_COUNT: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

pub fn reset_marks() {
    MARKS.with_borrow_mut(HashMap::clear);
    MARK_COUNT.set((0, 0));
}

pub fn register_receiver(key: MarkKey, stream: StreamRef) {
    MARKS.with_borrow_mut(|marks| marks.entry(key).or_default().receiver = Some(stream));
}

/// The sender marks the offset. A `request` is a frame of its own, which wakes the receiver when it arrives.
pub fn push_mark(key: MarkKey, offset: u64, request: bool) {
    let (marks, requests) = MARK_COUNT.get();
    MARK_COUNT.set((marks + u64::from(!request), requests + u64::from(request)));
    let receiver = MARKS.with_borrow_mut(|marks| {
        let channel = marks.entry(key).or_default();
        channel.marks.push_back(offset);
        channel.receiver.clone()
    });
    if request && let Some(receiver) = receiver {
        let one_way = ONE_WAY.get();
        tokio::spawn(async move {
            tokio::time::sleep(one_way).await;
            receiver.updates.wake();
        });
    }
}

/// Returns whether the receiver's application consumed a mark.
pub fn take_marks(key: MarkKey, consumed: u64) -> bool {
    MARKS.with_borrow_mut(|marks| {
        let Some(channel) = marks.get_mut(&key) else {
            return false;
        };
        let mut any = false;
        while channel.marks.front().is_some_and(|mark| *mark <= consumed) {
            channel.marks.pop_front();
            any = true;
        }
        any
    })
}
