use std::{
    io,
    num::NonZeroU32,
    ops::Not,
    task::{Poll, Waker},
};

use bytes::Bytes;

use crate::{
    error::StreamError,
    stream::{FinState, waker::WakerSlot},
};

pub struct OutboundTraffic(OutboundState);

impl OutboundTraffic {
    pub fn new(frame_limit: NonZeroU32, recv_window: u32) -> Self {
        Self(OutboundState::Open {
            ready_data: Default::default(),
            frame_limit,
            recv_window,
            writer: Default::default(),
        })
    }

    /// Polls the next outbound traffic update.
    ///
    /// Returns ready outbound frame data and `FIN_WRITE`.
    ///
    /// Notes:
    /// 1. If `FIN_WRITE` has already been sent, this method will return `([], false)`.
    /// 2. This method is not responsible for registering [`Waker`] for updates.
    pub fn poll_update(&mut self) -> Poll<(Bytes, bool)> {
        match &mut self.0 {
            OutboundState::Open {
                ready_data,
                frame_limit,
                recv_window,
                writer,
            } => {
                if ready_data.is_empty() || *recv_window == 0 {
                    return Poll::Pending;
                }
                let chunk_len = u32::try_from(ready_data.len())
                    .unwrap_or(u32::MAX)
                    .min(*recv_window)
                    .min(frame_limit.get());
                *recv_window -= chunk_len;
                let chunk = ready_data.split_to(crate::safe_cast_usize(chunk_len));
                if ready_data.is_empty() {
                    writer.wake();
                }
                Poll::Ready((chunk, false))
            },

            OutboundState::HalfClosed {
                ready_data,
                frame_limit,
                recv_window,
                writer,
            } => {
                if *recv_window == 0 {
                    return Poll::Pending;
                }
                let chunk_len = u32::try_from(ready_data.len())
                    .unwrap_or(u32::MAX)
                    .min(*recv_window)
                    .min(frame_limit.get());
                *recv_window -= chunk_len;
                let chunk = ready_data.split_to(crate::safe_cast_usize(chunk_len));
                let fin = if ready_data.is_empty() {
                    writer.wake();
                    self.0 = OutboundState::Closed {
                        recv_window: *recv_window,
                        fin_state: FinState {
                            sent: true,
                            received: false,
                        },
                    };
                    true
                } else {
                    false
                };
                Poll::Ready((chunk, fin))
            },

            OutboundState::Closed {
                fin_state: FinState { sent: true, .. },
                ..
            } => Poll::Pending,

            OutboundState::Closed { fin_state, .. } => {
                fin_state.sent = true;
                Poll::Ready((Default::default(), true))
            },
        }
    }

    pub fn close_writing(&mut self, updates_poller: &mut WakerSlot) {
        let OutboundState::Open {
            ready_data,
            frame_limit,
            recv_window,
            writer,
        } = &mut self.0
        else {
            return;
        };
        let new_state = if ready_data.is_empty() {
            updates_poller.wake();
            OutboundState::Closed {
                recv_window: *recv_window,
                fin_state: FinState {
                    sent: false,
                    received: false,
                },
            }
        } else {
            OutboundState::HalfClosed {
                ready_data: std::mem::take(ready_data),
                frame_limit: *frame_limit,
                recv_window: *recv_window,
                writer: std::mem::take(writer),
            }
        };
        self.0 = new_state;
    }

    pub fn poll_write_ready(&mut self, waker: &Waker) -> Poll<io::Result<()>> {
        let OutboundState::Open {
            ready_data, writer, ..
        } = &mut self.0
        else {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        };
        if ready_data.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            writer.register(waker);
            Poll::Pending
        }
    }

    pub fn poll_flushed(&mut self, waker: &Waker) -> Poll<()> {
        match &mut self.0 {
            OutboundState::Open {
                ready_data, writer, ..
            } => {
                if ready_data.is_empty() {
                    Poll::Ready(())
                } else {
                    writer.register(waker);
                    Poll::Pending
                }
            },

            OutboundState::HalfClosed { writer, .. } => {
                writer.register(waker);
                Poll::Pending
            },

            OutboundState::Closed { .. } => Poll::Ready(()),
        }
    }

    pub fn write(&mut self, data: Bytes, updates_poller: &mut WakerSlot) -> io::Result<()> {
        let OutboundState::Open {
            ready_data,
            recv_window,
            ..
        } = &mut self.0
        else {
            return Err(io::ErrorKind::BrokenPipe.into());
        };
        if ready_data.is_empty().not() {
            return Err(io::Error::other("pipe not ready"));
        }
        *ready_data = data;
        if ready_data.is_empty().not() && *recv_window > 0 {
            updates_poller.wake();
        }
        Ok(())
    }

    pub fn received_fin_read(&mut self, updates_poller: &mut WakerSlot) -> Result<(), StreamError> {
        match &mut self.0 {
            OutboundState::Open {
                writer,
                recv_window,
                ..
            }
            | OutboundState::HalfClosed {
                writer,
                recv_window,
                ..
            } => {
                writer.wake();
                updates_poller.wake();
                self.0 = OutboundState::Closed {
                    recv_window: *recv_window,
                    fin_state: FinState {
                        sent: false,
                        received: true,
                    },
                };
                Ok(())
            },

            OutboundState::Closed { fin_state, .. } => {
                if fin_state.received {
                    return Err(StreamError("sent duplicate FIN_READ"));
                }
                fin_state.received = true;
                Ok(())
            },
        }
    }

    pub fn received_window_update(
        &mut self,
        update: u32,
        updates_poller: &mut WakerSlot,
    ) -> Result<(), StreamError> {
        if update == 0 {
            return Ok(());
        }

        match &mut self.0 {
            OutboundState::Open {
                ready_data,
                recv_window,
                ..
            }
            | OutboundState::HalfClosed {
                ready_data,
                recv_window,
                ..
            } => {
                *recv_window = recv_window
                    .checked_add(update)
                    .ok_or(StreamError("triggered receive window overflow"))?;
                if *recv_window == update && ready_data.is_empty().not() {
                    updates_poller.wake();
                }
                Ok(())
            },
            OutboundState::Closed {
                recv_window,
                fin_state,
            } => {
                if fin_state.received {
                    return Err(StreamError("sent window update after FIN_READ"));
                }
                *recv_window = recv_window
                    .checked_add(update)
                    .ok_or(StreamError("triggered receive window overflow"))?;
                Ok(())
            },
        }
    }

    pub fn fin_state(&self) -> FinState {
        match &self.0 {
            OutboundState::Open { .. } | OutboundState::HalfClosed { .. } => FinState::default(),
            OutboundState::Closed { fin_state, .. } => *fin_state,
        }
    }
}

enum OutboundState {
    /// Traffic direction is fully open.
    Open {
        ready_data: Bytes,
        frame_limit: NonZeroU32,
        recv_window: u32,
        writer: WakerSlot,
    },
    /// Local writer closed, but there's still data to be flushed.
    HalfClosed {
        /// Never empty.
        ready_data: Bytes,
        frame_limit: NonZeroU32,
        recv_window: u32,
        writer: WakerSlot,
    },
    /// One of the sides closed and there's no pending data.
    Closed {
        recv_window: u32,
        fin_state: FinState,
    },
}
