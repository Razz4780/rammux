use std::task::{Poll, Waker};

use crate::{
    buffer::{Data, DataList},
    error::StreamError,
    global_pool::GlobalPool,
    stream::{FinState, waker::WakerSlot},
    transit,
};

/// The receiving direction of a stream.
///
/// The receive window is fixed: the stream may have `base_window` bytes unread,
/// and more only from the connection's pool, if the sender is allowed to use it (see `TRANSIT_CONTROL.md`).
/// The receiver does not tune anything. It counts what was received and not credited back (`unacked`),
/// checks the pool, and returns credit as the application reads.
pub struct InboundTraffic {
    state: InboundState,
    /// Credit is sent when this much accumulates.
    credit_threshold: u64,
    /// Bytes received on this stream and not credited back to the sender (yet).
    ///
    /// Never smaller than the same counter of the sender, see `TRANSIT_CONTROL.md`.
    unacked: Unacked,
    /// Bytes read by the application and not credited back yet.
    ///
    /// Always <= [`Self::unacked`].
    credit: u64,
}

impl InboundTraffic {
    pub fn new(base_window: u32) -> Self {
        Self {
            state: InboundState::Open {
                ready_data: Default::default(),
                reader: Default::default(),
            },
            credit_threshold: transit::credit_threshold(base_window),
            unacked: Unacked {
                base_window: u64::from(base_window),
                bytes: 0,
            },
            credit: 0,
        }
    }

    /// Polls the next inbound traffic update.
    ///
    /// Returns ready credit and `FIN_READ`.
    ///
    /// Notes:
    /// 1. If `FIN_READ` has already been sent, this method will return [`Poll::Pending`].
    /// 2. This method is not responsible for registering the [`Waker`].
    pub fn poll_update(&mut self, global: &mut GlobalPool) -> Poll<(u32, bool)> {
        match &mut self.state {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => {
                if self.credit < self.credit_threshold {
                    return Poll::Pending;
                }
                let credit = u32::try_from(self.credit).unwrap_or(u32::MAX);
                self.credit -= u64::from(credit);
                self.unacked
                    .set(global, self.unacked.bytes - u64::from(credit));
                Poll::Ready((credit, false))
            },

            InboundState::Closed { fin_state, .. } => {
                if fin_state.sent {
                    return Poll::Pending;
                }
                fin_state.sent = true;
                // The sender zeroes its counter when it reads `FIN_READ`, and the rest of the data is discarded.
                self.credit = 0;
                self.unacked.set(global, 0);
                Poll::Ready((0, true))
            },
        }
    }

    pub fn close_reading(&mut self, updates_poller: &mut WakerSlot) {
        match &mut self.state {
            InboundState::Open { .. } => {
                self.state = InboundState::Closed {
                    fin_state: FinState {
                        sent: false,
                        received: false,
                    },
                };
                updates_poller.wake();
            },

            InboundState::HalfClosed { .. } => {
                self.state = InboundState::Closed {
                    fin_state: FinState {
                        sent: false,
                        received: true,
                    },
                };
                updates_poller.wake();
            },

            InboundState::Closed { .. } => {},
        }
    }

    pub fn poll_read(
        &mut self,
        waker: &Waker,
        updates_poller: &mut WakerSlot,
    ) -> Poll<Option<Data>> {
        match &mut self.state {
            InboundState::Open { ready_data, reader } => {
                let Some(data) = ready_data.pop_front() else {
                    reader.register(waker);
                    return Poll::Pending;
                };
                self.credit += data.as_ref().len() as u64;
                if self.credit >= self.credit_threshold {
                    updates_poller.wake();
                }
                Poll::Ready(Some(data))
            },

            InboundState::HalfClosed { ready_data } => {
                let data = ready_data
                    .pop_front()
                    .expect("a half-closed stream with unread data has some chunk ready");
                self.credit += data.as_ref().len() as u64;
                if ready_data.is_empty() {
                    // Everything is read. The sender gets `FIN_READ` (and the stream's share of the pool back).
                    self.state = InboundState::Closed {
                        fin_state: FinState {
                            sent: false,
                            received: true,
                        },
                    };
                    updates_poller.wake();
                } else if self.credit >= self.credit_threshold {
                    updates_poller.wake();
                }
                Poll::Ready(Some(data))
            },

            InboundState::Closed { .. } => Poll::Ready(None),
        }
    }

    pub fn received_fin_write(
        &mut self,
        updates_poller: &mut WakerSlot,
    ) -> Result<(), StreamError> {
        match &mut self.state {
            InboundState::Open { ready_data, reader } => {
                self.state = if ready_data.is_empty() {
                    updates_poller.wake();
                    reader.wake();
                    InboundState::Closed {
                        fin_state: FinState {
                            sent: false,
                            received: true,
                        },
                    }
                } else {
                    InboundState::HalfClosed {
                        ready_data: std::mem::take(ready_data),
                    }
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

    pub fn received_data(
        &mut self,
        data: Data,
        global: &mut GlobalPool,
    ) -> Result<(), StreamError> {
        let len = data.as_ref().len() as u64;
        if len == 0 {
            return Ok(());
        }

        match &mut self.state {
            InboundState::Open { ready_data, reader } => {
                self.unacked.count(global, len)?;
                ready_data.push_back(data);
                reader.wake();
                Ok(())
            },

            InboundState::HalfClosed { .. }
            | InboundState::Closed {
                fin_state: FinState { received: true, .. },
            } => Err(StreamError("sent data after FIN_WRITE")),

            InboundState::Closed {
                fin_state:
                    FinState {
                        sent: false,
                        received: false,
                    },
            } => {
                // We stopped reading, but the sender does not know yet.
                // The data counts against the pool until `FIN_READ` is written, but we drop it.
                self.unacked.count(global, len)
            },

            InboundState::Closed {
                fin_state:
                    FinState {
                        sent: true,
                        received: false,
                    },
            } => {
                // The sender has not read our `FIN_READ` yet.
                Ok(())
            },
        }
    }

    pub fn fin_state(&self) -> FinState {
        match &self.state {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => FinState::default(),
            InboundState::Closed { fin_state, .. } => *fin_state,
        }
    }
}

/// Bytes received and not credited back, and the part of them that is owed to the pool.
struct Unacked {
    /// The base window `B`.
    base_window: u64,
    bytes: u64,
}

impl Unacked {
    /// Accounts for received data, and checks that the sender stays within the pool.
    fn count(&mut self, global: &mut GlobalPool, len: u64) -> Result<(), StreamError> {
        self.set(global, self.bytes + len);
        if global.rx_used > global.rx_pool {
            return Err(StreamError("exceeded the receive pool"));
        }
        Ok(())
    }

    /// Sets the counter, and keeps the pool usage in sync.
    fn set(&mut self, global: &mut GlobalPool, bytes: u64) {
        let old = transit::borrowed(self.bytes, self.base_window);
        let new = transit::borrowed(bytes, self.base_window);
        transit::rebalance(&mut global.rx_used, old, new);
        self.bytes = bytes;
    }
}

enum InboundState {
    /// Traffic direction is fully open.
    Open {
        ready_data: DataList,
        reader: WakerSlot,
    },
    /// `FIN_WRITE` was received, but the local reader is still draining the data.
    ///
    /// We only send `FIN_READ` and allow for the stream to be closed
    /// when the reader has consumed all pending data.
    HalfClosed {
        // Never empty.
        ready_data: DataList,
    },
    /// One of the sides closed and there's no pending data.
    Closed { fin_state: FinState },
}
