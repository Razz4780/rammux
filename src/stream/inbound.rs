use std::{
    num::NonZeroU32,
    ops::Not,
    task::{Poll, Waker},
    time::Duration,
};

use tokio::time::Instant;

use crate::{
    buffer::{Data, DataList},
    error::StreamError,
    global_pool::GlobalPool,
    stream::{FinState, waker::WakerSlot},
};

pub struct InboundTraffic(InboundState);

impl InboundTraffic {
    pub fn new(recv_window: NonZeroU32) -> Self {
        Self(InboundState::Open {
            ready_data: Default::default(),
            reader: Default::default(),
            recv_window: RecvWindow {
                initial: recv_window,
                current: recv_window.get(),
                consumed: 0,
                freed: 0,
                last_update: Instant::now(),
            },
        })
    }

    /// Polls the next inbound traffic update.
    ///
    /// Returns ready receive window update and `FIN_READ`.
    ///
    /// Notes:
    /// 1. If `FIN_READ` has already been sent, this method will return [`Poll::Pending`].
    /// 2. This method is not responsible for registering the [`Waker`].
    pub fn poll_update(&mut self, global: &mut GlobalPool) -> Poll<(u32, bool)> {
        match &mut self.0 {
            InboundState::Open { recv_window, .. } => {
                let update = recv_window.try_update(global);
                if update == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready((update, false))
                }
            },

            InboundState::HalfClosed { freed_borrow, .. } => {
                global.available += crate::safe_cast_usize(std::mem::take(freed_borrow));
                Poll::Pending
            },

            InboundState::Closed {
                stale_borrow,
                fin_state,
                ..
            } => {
                global.available += crate::safe_cast_usize(std::mem::take(stale_borrow));
                if fin_state.sent {
                    Poll::Pending
                } else {
                    fin_state.sent = true;
                    Poll::Ready((0, true))
                }
            },
        }
    }

    pub fn close_reading(&mut self, updates_poller: &mut WakerSlot) {
        match &mut self.0 {
            InboundState::Open { recv_window, .. } => {
                self.0 = InboundState::Closed {
                    stale_borrow: recv_window.borrowed(),
                    remaining_recv_window: recv_window.remaining(),
                    fin_state: FinState {
                        sent: false,
                        received: false,
                    },
                };
                updates_poller.wake();
            },

            InboundState::HalfClosed {
                borrow,
                freed_borrow,
                ..
            } => {
                self.0 = InboundState::Closed {
                    stale_borrow: *borrow + *freed_borrow,
                    remaining_recv_window: 0,
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
        match &mut self.0 {
            InboundState::Open {
                ready_data,
                reader,
                recv_window,
            } => {
                let Some(data) = ready_data.pop_front() else {
                    reader.register(waker);
                    return Poll::Pending;
                };
                let freed = u32::try_from(data.as_ref().len()).expect("ooga booga");
                recv_window.freed += freed;
                if recv_window.can_update() {
                    updates_poller.wake();
                }
                Poll::Ready(Some(data))
            },

            InboundState::HalfClosed {
                ready_data,
                borrow,
                freed_borrow,
            } => {
                let data = ready_data.pop_front().expect("ooga booga");
                let freed = u32::try_from(data.as_ref().len())
                    .expect("ooga booga")
                    .min(*borrow);
                *freed_borrow += freed;
                if ready_data.is_empty() {
                    self.0 = InboundState::Closed {
                        stale_borrow: *freed_borrow,
                        remaining_recv_window: 0,
                        fin_state: FinState {
                            sent: false,
                            received: true,
                        },
                    };
                    updates_poller.wake();
                } else {
                    *borrow -= freed;
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
        match &mut self.0 {
            InboundState::Open {
                ready_data,
                reader,
                recv_window,
            } => {
                self.0 = if ready_data.is_empty() {
                    updates_poller.wake();
                    reader.wake();
                    InboundState::Closed {
                        stale_borrow: recv_window.borrowed(),
                        remaining_recv_window: 0,
                        fin_state: FinState {
                            sent: false,
                            received: true,
                        },
                    }
                } else {
                    InboundState::HalfClosed {
                        ready_data: std::mem::take(ready_data),
                        borrow: recv_window.borrowed(),
                        freed_borrow: 0,
                    }
                };
                Ok(())
            },

            InboundState::HalfClosed { .. }
            | InboundState::Closed {
                fin_state: FinState { received: true, .. },
                ..
            } => Err(StreamError("sent duplicate FIN_WRITE")),

            InboundState::Closed { fin_state, .. } => {
                fin_state.received = true;
                Ok(())
            },
        }
    }

    pub fn received_data(&mut self, data: Data) -> Result<(), StreamError> {
        if data.as_ref().is_empty() {
            return Ok(());
        }

        match &mut self.0 {
            InboundState::Open {
                ready_data,
                reader,
                recv_window,
            } => {
                let data_len = u32::try_from(data.as_ref().len()).expect("ooga booga");
                if recv_window.remaining() < data_len {
                    return Err(StreamError("exceeded available receive window"));
                }
                recv_window.consumed += data_len;
                ready_data.push_back(data);
                reader.wake();
                Ok(())
            },

            InboundState::HalfClosed { .. }
            | InboundState::Closed {
                fin_state: FinState { received: true, .. },
                ..
            } => Err(StreamError("sent data after FIN_WRITE")),

            InboundState::Closed {
                fin_state: FinState {
                    received: false, ..
                },
                remaining_recv_window,
                ..
            } => {
                let data_len = u32::try_from(data.as_ref().len()).expect("ooga booga");
                *remaining_recv_window = remaining_recv_window
                    .checked_sub(data_len)
                    .ok_or(StreamError("exceeded available receive window"))?;
                Ok(())
            },
        }
    }

    pub fn fin_state(&self) -> FinState {
        match &self.0 {
            InboundState::Open { .. } | InboundState::HalfClosed { .. } => FinState::default(),
            InboundState::Closed { fin_state, .. } => *fin_state,
        }
    }
}

/// Tracks the state of a receive window for an open inbound traffic direction.
struct RecvWindow {
    /// Initial size of the receive window, immutable.
    ///
    /// This is taken from the [`RammuxConfig`](crate::config::RammuxConfig)
    /// and same for all virtual streams.
    initial: NonZeroU32,
    /// Current size of the receive window, including the part already consumed by the remote writer.
    ///
    /// Always >= [`Self::initial`].
    current: u32,
    /// Part of the current receive window consumed by the remote writer.
    ///
    /// Always <= [`Self::current`].
    consumed: u32,
    /// Part of the current receive window consumed by the remote writer and freed by the local reader.
    ///
    /// Always <= [`Self::consumed`].
    freed: u32,
    /// Time of the last produced window update.
    last_update: Instant,
}

impl RecvWindow {
    fn try_update(&mut self, global: &mut GlobalPool) -> u32 {
        if self.can_update().not() {
            return 0;
        }

        let optimal = global
            .rtt
            .map(|rtt| Self::get_optimal(self.freed, self.last_update.elapsed(), rtt))
            .unwrap_or(self.current);
        let clamped = optimal
            // Window cannot shrink below the initial size.
            .max(self.initial.get())
            // Window cannot shrink by more than 25% in one round.
            .max(self.current - self.current / 4)
            // Window cannot grow by more than 100% in one round.
            .min(self.current.saturating_mul(2));

        let update = if clamped > self.current {
            // Window can grow, we need to borrow from the global pool.
            let wanted_borrow = clamped - self.current;
            let borrow = {
                let borrow = u32::try_from(global.available)
                    .map(|available| available.min(wanted_borrow))
                    .unwrap_or(wanted_borrow);
                global.available -= crate::safe_cast_usize(borrow);
                borrow
            };
            self.current += borrow;
            self.freed + borrow
        } else if clamped < self.current {
            let excess = self.current - clamped;
            global.available += crate::safe_cast_usize(excess);
            self.current = clamped;
            self.freed - excess
        } else {
            self.freed
        };

        self.last_update = Instant::now();
        self.consumed = 0;
        self.freed = 0;

        update
    }

    fn can_update(&self) -> bool {
        self.freed >= self.current / 2
    }

    fn remaining(&self) -> u32 {
        self.current - self.consumed
    }

    fn borrowed(&self) -> u32 {
        self.current - self.initial.get()
    }

    /// Returns an optimal window size for the given parameters.
    ///
    /// # Casting
    ///
    /// This function produces the next window as [`f64`] and casts it to [`u32`].
    /// This cast can truncate, or produce a 0-sized result if the given durations
    /// are very small/large and the calculated [`f64`] ends up being something like [`f64::NAN`].
    /// However, given that [`Self::try_update`] applies bounds to window resizes,
    /// this behavior is acceptable.
    #[allow(clippy::cast_possible_truncation)]
    fn get_optimal(consumed_since_update: u32, time_since_update: Duration, rtt: Duration) -> u32 {
        let new_window = 1.5 * rtt.as_secs_f64() * f64::from(consumed_since_update)
            / time_since_update.as_secs_f64();
        new_window as u32
    }
}

enum InboundState {
    /// Traffic direction is fully open.
    Open {
        ready_data: DataList,
        reader: WakerSlot,
        recv_window: RecvWindow,
    },
    /// `FIN_WRITE` was received, but the local reader is still draining the data.
    ///
    /// We only send `FIN_READ` and allow for the stream to be closed
    /// when the reader has consumed all pending data.
    HalfClosed {
        // Never empty.
        ready_data: DataList,
        borrow: u32,
        freed_borrow: u32,
    },
    /// One of the sides closed and there's no pending data.
    Closed {
        stale_borrow: u32,
        remaining_recv_window: u32,
        fin_state: FinState,
    },
}
