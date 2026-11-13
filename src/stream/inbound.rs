//! The receiving direction of a stream.

use std::task::{Poll, Waker};

use futures::task::AtomicWaker;

use crate::{
    buffer::{Data, DataList},
    error::StreamError,
    flow::{self, Pool, Window},
    stream::{FinState, waker::WakerSlot},
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
        if self.credit >= self.credit_threshold {
            updates.wake();
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
    pub fn poll_update(&mut self, pool: &mut Pool) -> Poll<(u32, bool)> {
        match &mut self.state {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => {
                if self.credit < self.credit_threshold {
                    return Poll::Pending;
                }
                let credit = u32::try_from(self.credit).unwrap_or(u32::MAX);
                self.credit -= u64::from(credit);
                self.window
                    .set(pool, self.window.unacked() - u64::from(credit));
                Poll::Ready((credit, false))
            },

            InboundState::Closed { fin_state } => {
                if fin_state.sent {
                    return Poll::Pending;
                }
                fin_state.sent = true;
                // The sender forgets the bytes in flight when it reads `FIN_READ`, and the rest of the data is discarded.
                self.credit = 0;
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

/// Accounts for received data, and checks that the sender stays within the pool.
fn count(window: &mut Window, pool: &mut Pool, len: u64) -> Result<(), StreamError> {
    window.add(pool, len);
    if pool.is_overdrawn() {
        return Err(StreamError("exceeded the receive pool"));
    }
    Ok(())
}
