//! The receiving direction of a stream.

use std::task::{Poll, Waker};

use futures::task::AtomicWaker;
use tokio::time::Instant;

use crate::{
    buffer::{Data, DataList},
    error::StreamError,
    flow::{self, CREDIT_STEP_DIVISOR, Pool, RateEstimate, Window},
    stream::{FinState, RxFlow, waker::WakerSlot},
};

/// The receiving direction of a stream.
///
/// The receive window is fixed: the stream may have `B` bytes unread, and more only from the connection's pool,
/// if the sender is allowed to use it (see `TRANSIT_CONTROL.md`). The receiver does not tune it.
/// It counts what was received and not credited back, checks the pool, and returns credit as the application reads.
///
/// Credit goes back in steps. The step is `T_c` and grows with the pace of the reader to
/// `1 / CREDIT_STEP_DIVISOR` of what it consumes per loop, so a fast stream sends a few large `WINDOW`s per loop
/// instead of many small ones. A step larger than `T_c` must not strand the credit of a reader that stops between
/// two steps: the sender would count those bytes as in flight for good, and with a full window it could never send
/// again. So a stream with such a step falls back to `T_c` when its reader leaves its pace, and the connection's
/// [`Settle`](super::conn_flow::Settle) timer makes sure that somebody looks at it soon after it goes quiet.
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
    /// Credit is sent when this much accumulates. At least [`Self::min_step`].
    ///
    /// Only the connection side changes it. The application side compares with it only, it has no clock.
    step: u64,
    /// `T_c`, the smallest step.
    min_step: u64,
    /// The credit sent since the first round trip sample, if any was.
    sent: Option<SentCredit>,
}

/// The credit that a receiving direction sent.
struct SentCredit {
    /// The rate of the credit, with the smoothed round trip time as the time constant.
    rate: RateEstimate,
    /// When the last credit was sent.
    last: Instant,
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
            step: flow::credit_threshold(base_window),
            min_step: flow::credit_threshold(base_window),
            sent: None,
        }
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
    ///
    /// Wakes the `credit_task` of the stream when the read makes credit or `FIN_READ` due.
    pub fn poll_read(&mut self, waker: &Waker, credit_task: &AtomicWaker) -> Poll<Option<Data>> {
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
                    credit_task.wake();
                }
                data
            },

            InboundState::Closed { .. } => return Poll::Ready(None),
        };

        self.credit += data.as_ref().len() as u64;
        if self.credit >= self.step {
            credit_task.wake();
        }
        Poll::Ready(Some(data))
    }

    /// The application stops reading. `FIN_READ` follows, and the data that arrives is discarded.
    ///
    /// Wakes the `credit_task` of the stream, which sends `FIN_READ`.
    pub fn close_reading(&mut self, credit_task: &AtomicWaker) {
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
        credit_task.wake();
    }

    // The connection side.

    /// Polls for what this direction sends next: credit, and whether it is `FIN_READ`.
    ///
    /// Returns [`Poll::Pending`] when there is nothing to send. Registering the waker with the stream is up to
    /// the caller. While the step is larger than `T_c`, the waker is also registered with
    /// [`RxFlow::settle`], whatever this returns: the reader may stop before the next step, and then only the
    /// timer looks at the stream again.
    pub fn poll_update(
        &mut self,
        rx: &mut RxFlow,
        now: Instant,
        waker: &Waker,
    ) -> Poll<(u32, bool)> {
        match &mut self.state {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => {
                if self.step > self.min_step
                    && let (Some(after), Some(sent)) = (rx.settle_after(), &self.sent)
                    && now.saturating_duration_since(sent.last) >= after
                {
                    // The reader left its pace. What it consumed since the last credit goes back
                    // once it reaches `T_c`, and a reader that picks up again starts from there.
                    self.step = self.min_step;
                }
                if self.credit < self.step {
                    self.settle(rx, waker);
                    return Poll::Pending;
                }

                let credit = u32::try_from(self.credit).unwrap_or(u32::MAX);
                self.credit -= u64::from(credit);
                self.window
                    .set(&mut rx.pool, self.window.unacked() - u64::from(credit));
                if let Some(srtt) = rx.srtt.secs() {
                    let sent = self.sent.get_or_insert_with(|| SentCredit {
                        rate: RateEstimate::new(now),
                        last: now,
                    });
                    sent.rate.add(credit, now, srtt);
                    sent.last = now;
                    // Whole bytes of what the reader consumed over the last loop, as the sender estimates it.
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let step = (sent.rate.value(now, srtt) * srtt / CREDIT_STEP_DIVISOR) as u64;
                    self.step = step.max(self.min_step);
                }
                self.settle(rx, waker);
                Poll::Ready((credit, false))
            },

            InboundState::Closed { fin_state } => {
                if fin_state.sent {
                    return Poll::Pending;
                }
                fin_state.sent = true;
                // The sender forgets the bytes in flight when it reads `FIN_READ`, and the rest of the data is discarded.
                self.credit = 0;
                self.window.set(&mut rx.pool, 0);
                Poll::Ready((0, true))
            },
        }
    }

    /// Makes sure that the stream is polled again soon, while it holds credit back in a step larger than `T_c`.
    ///
    /// Below `T_c` nothing is needed: the sender always has room in its base window for more data,
    /// and the reads of that data wake the stream.
    fn settle(&self, rx: &mut RxFlow, waker: &Waker) {
        if self.step > self.min_step
            && let Some(period) = rx.settle_period()
        {
            rx.settle.register(waker, period);
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
    ///
    /// Wakes the `credit_task` of the stream if `FIN_READ` is due now: nothing is left to read.
    pub fn received_fin_write(&mut self, credit_task: &AtomicWaker) -> Result<(), StreamError> {
        match &mut self.state {
            InboundState::Open { ready_data, .. } if !ready_data.is_empty() => {
                self.state = InboundState::HalfClosed {
                    ready_data: std::mem::take(ready_data),
                };
                Ok(())
            },

            InboundState::Open { reader, .. } => {
                credit_task.wake();
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

    /// Returns the bytes read by the application and not credited back yet.
    #[cfg(test)]
    pub fn pending_credit(&self) -> u64 {
        self.credit
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
