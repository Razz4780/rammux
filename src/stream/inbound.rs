//! The receiving direction of a stream.

use std::task::{Poll, Waker};

use futures::task::AtomicWaker;

use tokio::time::Instant;

use crate::{
    buffer::{Data, DataList},
    error::StreamError,
    flow::{self, Pool, RateEstimate, Window},
    stream::{
        ConnFlow, FinState, StreamRef,
        credit::{self, CreditPolicy, EXPERIMENT, MarkKey},
        waker::WakerSlot,
    },
};

/// The receiving direction of a stream.
///
/// The receive window is fixed: the stream may have `B` bytes unread, and more only from the connection's pool,
/// if the sender is allowed to use it (see `TRANSIT_CONTROL.md`). The receiver does not tune anything.
/// It counts what was received and not credited back, checks the pool, and returns credit as the application reads.
pub struct InboundTraffic {
    state: InboundState,
    /// Bytes received and not credited back.
    ///
    /// Never larger than the same counter of the sender: bytes in transit count at the sender only,
    /// credit is subtracted here before it reaches the sender, and `FIN_READ` zeroes this counter first.
    /// So a sender that stays within the pool by its own count never overdraws it by ours.
    window: Window,
    /// Bytes read by the application and not credited back yet. Never more than the bytes in [`Self::window`].
    credit: u64,
    /// Credit is sent when this much accumulates.
    credit_threshold: u64,
    /// EXPERIMENT: the connection is woken at this much credit.
    wake_at: u64,
    /// EXPERIMENT: the part of [`Self::credit`] that the connection side has seen.
    seen: u64,
    /// EXPERIMENT: what the application consumed, as a rate.
    consumed: RateEstimate,
    /// EXPERIMENT: the stream is in the list of waiters.
    waiting: bool,
    /// EXPERIMENT: the sender's marks, the bytes consumed so far, and whether a mark was consumed.
    marks: Option<MarkKey>,
    consumed_total: u64,
    marked: bool,
    /// EXPERIMENT: counts the streams with credit due, and whether this one is counted.
    control_pending: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    counted: bool,
}

enum InboundState {
    /// The peer may send.
    Open {
        /// Data received and not read by the application yet.
        ready_data: DataList,
        /// Waits for [`Self::Open::ready_data`].
        reader: WakerSlot,
    },
    /// `FIN_WRITE` was received, and the application still reads the rest of the data.
    ///
    /// `FIN_READ` is sent, and the stream can close, only once everything is read.
    HalfClosed {
        /// Never empty.
        ready_data: DataList,
    },
    /// Nothing more will be read. `FIN_READ` is sent if it was not sent yet.
    Closed { fin_state: FinState },
}

impl InboundTraffic {
    pub fn new(base_window: u32) -> Self {
        Self {
            state: InboundState::Open {
                ready_data: DataList::default(),
                reader: WakerSlot::default(),
            },
            window: Window::new(base_window),
            credit: 0,
            credit_threshold: flow::credit_threshold(base_window),
            wake_at: floor(
                EXPERIMENT.get().policy != CreditPolicy::Fixed,
                flow::credit_threshold(base_window),
            ),
            seen: 0,
            consumed: RateEstimate::new(Instant::now()),
            waiting: false,
            marks: None,
            consumed_total: 0,
            marked: false,
            control_pending: None,
            counted: false,
        }
    }

    pub fn set_control_pending(&mut self, counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        self.control_pending = Some(counter);
    }

    fn count_due(&mut self, due: bool) {
        use std::sync::atomic::Ordering;
        if due == self.counted {
            return;
        }
        if let Some(counter) = &self.control_pending {
            if due {
                counter.fetch_add(1, Ordering::Relaxed);
            } else {
                counter.fetch_sub(1, Ordering::Relaxed);
            }
            self.counted = due;
        }
    }

    pub fn set_marks(&mut self, key: MarkKey) {
        self.marks = Some(key);
    }

    /// Returns the `FIN_*` state of this direction.
    pub fn fin_state(&self) -> FinState {
        match &self.state {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => FinState::default(),
            InboundState::Closed { fin_state } => *fin_state,
        }
    }

    // The application side.

    /// Returns the next chunk of data for the application, or [`None`] at the end of the stream.
    pub fn poll_read(&mut self, waker: &Waker, updates: &AtomicWaker) -> Poll<Option<Data>> {
        let data = match &mut self.state {
            InboundState::Open { ready_data, reader } => {
                let Some(data) = ready_data.pop_front() else {
                    reader.register(waker);
                    return Poll::Pending;
                };
                data
            },

            InboundState::HalfClosed { ready_data } => {
                let data = ready_data
                    .pop_front()
                    .expect("a half-closed direction has data to read");
                if ready_data.is_empty() {
                    // Everything is read. The sender gets `FIN_READ` (and the stream's share of the pool back).
                    self.state = InboundState::Closed {
                        fin_state: FinState {
                            sent: false,
                            received: true,
                        },
                    };
                    updates.wake();
                }
                data
            },

            InboundState::Closed { .. } => return Poll::Ready(None),
        };

        self.credit += data.as_ref().len() as u64;
        if self.credit >= self.wake_at {
            updates.wake();
        }
        if self.credit >= self.credit_threshold {
            self.count_due(true);
        }
        Poll::Ready(Some(data))
    }

    /// The application stops reading. `FIN_READ` follows, and the data that arrives is discarded.
    pub fn close_reading(&mut self, updates: &AtomicWaker) {
        let received = match &self.state {
            InboundState::Open { .. } => false,
            InboundState::HalfClosed { .. } => true,
            InboundState::Closed { .. } => return,
        };
        self.state = InboundState::Closed {
            fin_state: FinState {
                sent: false,
                received,
            },
        };
        updates.wake();
    }

    // The connection side.

    /// Polls for what this direction sends next: credit, and whether it is `FIN_READ`.
    ///
    /// Returns [`Poll::Pending`] when there is nothing to send. Registering the waker is up to the caller.
    pub fn poll_update(
        &mut self,
        flow: &mut ConnFlow,
        me: &StreamRef,
        now: Instant,
    ) -> Poll<(u32, bool)> {
        let pool = &mut flow.rx_pool;
        match &mut self.state {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => {
                let srtt = flow.srtt.get();
                let fresh = self.credit - self.seen;
                self.seen = self.credit;
                if fresh > 0
                    && let Some(srtt) = srtt
                {
                    self.consumed
                        .add_amount(fresh as f64, now, srtt.as_secs_f64());
                }
                self.consumed_total += fresh;
                if let Some(key) = self.marks
                    && credit::take_marks(key, self.consumed_total)
                {
                    self.marked = true;
                }
                let gate = &mut flow.credit;
                if self.credit < floor(gate.riders(), self.credit_threshold)
                    && !(self.marked && self.credit > 0)
                {
                    return Poll::Pending;
                }
                let due_at = match gate.policy {
                    CreditPolicy::Proportional { div, .. } => {
                        let volume = srtt.map_or(0.0, |srtt| {
                            let srtt = srtt.as_secs_f64();
                            self.consumed.value(now, srtt) * srtt
                        });
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        self.credit_threshold.max((volume / div) as u64)
                    },
                    _ => self.credit_threshold,
                };
                let due = match gate.policy {
                    CreditPolicy::Marks { .. } => self.marked,
                    _ => self.credit >= due_at,
                };
                let send = gate.flushing || (due && gate.request(now, srtt));
                if !send {
                    if !self.waiting {
                        self.waiting = true;
                        gate.wait(me);
                    }
                    if self.credit >= self.credit_threshold {
                        gate.tail(now, srtt);
                    }
                    return Poll::Pending;
                }
                self.waiting = false;
                self.marked = false;
                self.count_due(false);
                let credit = u32::try_from(self.credit).unwrap_or(u32::MAX);
                self.credit -= u64::from(credit);
                self.seen = self.credit;
                self.window
                    .set(pool, self.window.unacked() - u64::from(credit));
                Poll::Ready((credit, false))
            },

            InboundState::Closed { fin_state } => {
                if self.counted {
                    if let Some(counter) = &self.control_pending {
                        counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    self.counted = false;
                }
                if fin_state.sent {
                    return Poll::Pending;
                }
                fin_state.sent = true;
                // The sender forgets the bytes in flight when it reads `FIN_READ`, and the rest of the data is discarded.
                self.credit = 0;
                self.seen = 0;
                self.window.set(pool, 0);
                Poll::Ready((0, true))
            },
        }
    }

    /// The peer sent data.
    pub fn received_data(&mut self, data: Data, pool: &mut Pool) -> Result<(), StreamError> {
        let len = data.as_ref().len() as u64;
        if len == 0 {
            return Ok(());
        }

        match &mut self.state {
            InboundState::Open { ready_data, reader } => {
                count(&mut self.window, pool, len)?;
                ready_data.push_back(data);
                reader.wake();
                Ok(())
            },

            InboundState::HalfClosed { .. }
            | InboundState::Closed {
                fin_state: FinState { received: true, .. },
            } => Err(StreamError("sent data after FIN_WRITE")),

            InboundState::Closed {
                fin_state: FinState { sent: false, .. },
            } => {
                // We stopped reading, but the sender does not know yet.
                // The data counts against the pool until `FIN_READ` is written, but we drop it.
                count(&mut self.window, pool, len)
            },

            InboundState::Closed {
                fin_state: FinState { sent: true, .. },
            } => {
                // The sender has not read our `FIN_READ` yet.
                Ok(())
            },
        }
    }

    /// The peer stopped writing.
    pub fn received_fin_write(&mut self, updates: &AtomicWaker) -> Result<(), StreamError> {
        match &mut self.state {
            InboundState::Open { ready_data, .. } if !ready_data.is_empty() => {
                self.state = InboundState::HalfClosed {
                    ready_data: std::mem::take(ready_data),
                };
                Ok(())
            },

            InboundState::Open { reader, .. } => {
                updates.wake();
                reader.wake();
                self.state = InboundState::Closed {
                    fin_state: FinState {
                        sent: false,
                        received: true,
                    },
                };
                Ok(())
            },

            InboundState::HalfClosed { .. }
            | InboundState::Closed {
                fin_state: FinState { received: true, .. },
            } => Err(StreamError("sent duplicate FIN_WRITE")),

            InboundState::Closed { fin_state } => {
                fin_state.received = true;
                Ok(())
            },
        }
    }

    #[cfg(test)]
    pub fn unacked(&self) -> u64 {
        self.window.unacked()
    }
}

/// EXPERIMENT: the least credit that rides along with a flush.
fn floor(riders: bool, threshold: u64) -> u64 {
    if riders {
        (threshold / 8).max(1)
    } else {
        threshold
    }
}

/// Accounts for received data, and checks that the sender stays within the pool.
fn count(window: &mut Window, pool: &mut Pool, len: u64) -> Result<(), StreamError> {
    window.add(pool, len);
    if pool.is_overdrawn() {
        return Err(StreamError("exceeded the receive pool"));
    }
    Ok(())
}
