//! The sending direction of a stream.

use std::{
    io,
    num::{NonZeroU16, NonZeroU32},
    task::{Poll, Waker},
};

use bytes::Bytes;
use futures::task::AtomicWaker;
use tokio::time::Instant;

use crate::{
    error::StreamError,
    flow::{Loan, RateEstimate, Window},
    stream::{ConnFlow, FinState, StreamRef, waker::WakerSlot},
};

/// What the sending direction of a stream needs from the rest of the connection.
pub struct Ctx<'a> {
    /// State shared by the streams of the connection.
    pub flow: &'a mut ConnFlow,
    /// This stream, as the turn queue knows it. The caller holds the lock of its state.
    pub me: &'a StreamRef,
    /// The time.
    pub now: Instant,
}

impl Ctx<'_> {
    /// Wakes the [`StreamUpdates`](super::updates::StreamUpdates) of this stream.
    fn wake_updates(&self) {
        self.me.updates.wake();
    }
}

/// The sending direction of a stream.
///
/// The receiver does not tune its windows. This side decides how much of the peer's pool
/// the stream uses: in proportion to the rate its reader has proven, and when its turn comes.
/// See `TRANSIT_CONTROL.md` for the rules and the reasons.
pub struct OutboundTraffic {
    state: OutboundState,
    ledger: Ledger,
}

enum OutboundState {
    /// The application may write.
    Open {
        /// Data written by the application and not sent yet.
        ready_data: Bytes,
        /// Waits for [`Self::Open::ready_data`] to be sent.
        writer: WakerSlot,
    },
    /// The application closed writing, and there is still data to send before `FIN_WRITE`.
    HalfClosed {
        /// Never empty.
        ready_data: Bytes,
        writer: WakerSlot,
    },
    /// Nothing more will be sent but `FIN_WRITE`, if it was not sent yet.
    Closed { fin_state: FinState },
}

impl OutboundTraffic {
    pub fn new(frame_limit: NonZeroU16, base_window: u32) -> Self {
        Self {
            state: OutboundState::Open {
                ready_data: Bytes::new(),
                writer: WakerSlot::default(),
            },
            ledger: Ledger {
                window: Window::new(base_window),
                frame_limit: u64::from(frame_limit.get()),
                loan: Loan::default(),
                rate: RateEstimate::new(Instant::now()),
            },
        }
    }

    /// Returns the `FIN_*` state of this direction.
    pub fn fin_state(&self) -> FinState {
        match &self.state {
            OutboundState::Open { .. } | OutboundState::HalfClosed { .. } => FinState::default(),
            OutboundState::Closed { fin_state } => *fin_state,
        }
    }

    // The application side.

    /// Returns whether the application can write.
    pub fn poll_write_ready(&mut self, waker: &Waker) -> Poll<io::Result<()>> {
        let OutboundState::Open { ready_data, writer } = &mut self.state else {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        };
        if ready_data.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            writer.register(waker);
            Poll::Pending
        }
    }

    /// Takes data from the application. The direction must be [ready](Self::poll_write_ready).
    pub fn write(&mut self, data: Bytes, updates: &AtomicWaker) -> io::Result<()> {
        let OutboundState::Open { ready_data, .. } = &mut self.state else {
            return Err(io::ErrorKind::BrokenPipe.into());
        };
        if !ready_data.is_empty() {
            return Err(io::Error::other("pipe not ready"));
        }
        if !data.is_empty() {
            *ready_data = data;
            updates.wake();
        }
        Ok(())
    }

    /// Returns whether everything the application wrote was handed to the connection.
    pub fn poll_flushed(&mut self, waker: &Waker) -> Poll<()> {
        match &mut self.state {
            OutboundState::Open { ready_data, .. } if ready_data.is_empty() => Poll::Ready(()),
            OutboundState::Open { writer, .. } | OutboundState::HalfClosed { writer, .. } => {
                writer.register(waker);
                Poll::Pending
            },
            OutboundState::Closed { .. } => Poll::Ready(()),
        }
    }

    /// The application closes writing. `FIN_WRITE` follows the data that is not sent yet.
    pub fn close_writing(&mut self, updates: &AtomicWaker) {
        let OutboundState::Open { ready_data, writer } = &mut self.state else {
            return;
        };
        self.state = if ready_data.is_empty() {
            updates.wake();
            OutboundState::Closed {
                fin_state: FinState::default(),
            }
        } else {
            OutboundState::HalfClosed {
                ready_data: std::mem::take(ready_data),
                writer: std::mem::take(writer),
            }
        };
    }

    // The connection side.

    /// Polls for what this direction sends next: a chunk of data, and whether `FIN_WRITE` follows it.
    ///
    /// Returns [`Poll::Pending`] when there is nothing to send. Registering the waker is up to the caller.
    pub fn poll_update(&mut self, ctx: &mut Ctx<'_>) -> Poll<(Bytes, bool)> {
        match &mut self.state {
            OutboundState::Open { ready_data, writer } => {
                let Some(len) = self.ledger.plan(ctx, ready_data.len()) else {
                    return Poll::Pending;
                };
                let chunk = ready_data.split_to(len);
                if ready_data.is_empty() {
                    writer.wake();
                }
                Poll::Ready((chunk, false))
            },

            OutboundState::HalfClosed { ready_data, writer } => {
                let Some(len) = self.ledger.plan(ctx, ready_data.len()) else {
                    return Poll::Pending;
                };
                let chunk = ready_data.split_to(len);
                if !ready_data.is_empty() {
                    return Poll::Ready((chunk, false));
                }
                writer.wake();
                self.state = OutboundState::Closed {
                    fin_state: FinState {
                        sent: true,
                        received: false,
                    },
                };
                Poll::Ready((chunk, true))
            },

            OutboundState::Closed { fin_state } => {
                // A closed stream must not hold up the others.
                ctx.flow.pass_turn(ctx.me);
                if fin_state.sent {
                    return Poll::Pending;
                }
                fin_state.sent = true;
                Poll::Ready((Bytes::new(), true))
            },
        }
    }

    /// The peer returned credit. The bytes go back to the shared pool, not to this stream.
    pub fn received_window_update(
        &mut self,
        credit: NonZeroU32,
        ctx: &mut Ctx<'_>,
    ) -> Result<(), StreamError> {
        let credit = credit.get();
        if let OutboundState::Closed {
            fin_state: FinState { received: true, .. },
        } = &self.state
        {
            return Err(StreamError("sent window update after FIN_READ"));
        }
        let unacked = self
            .ledger
            .window
            .unacked()
            .checked_sub(u64::from(credit))
            .ok_or(StreamError("returned more credit than was in flight"))?;

        let freed = self.ledger.window.set(&mut ctx.flow.tx_pool, unacked);
        // The credit pays back the advance first.
        self.ledger
            .loan
            .repay(&mut ctx.flow.advance, u64::from(credit));
        // The estimate only runs once the loop time is known, or its time constant is not.
        if let Some(srtt) = ctx.flow.srtt.secs() {
            self.ledger.rate.add(credit, ctx.now, srtt);
        }
        if freed {
            ctx.flow.turns.wake_front();
        }
        // A larger allowance, and the stream may have been waiting for it.
        if let OutboundState::Open { ready_data, .. } | OutboundState::HalfClosed { ready_data, .. } =
            &self.state
            && !ready_data.is_empty()
        {
            ctx.wake_updates();
        }
        Ok(())
    }

    /// The peer stopped reading. The bytes in flight are forgotten,
    /// which returns the share of the pool that the stream used.
    pub fn received_fin_read(&mut self, ctx: &mut Ctx<'_>) -> Result<(), StreamError> {
        self.close_reading_side(&ctx.me.updates)?;
        if self.ledger.window.set(&mut ctx.flow.tx_pool, 0) {
            ctx.flow.turns.wake_front();
        }
        // The whole advance goes back, wherever the stream was in its episode.
        self.ledger.loan.release(&mut ctx.flow.advance);
        Ok(())
    }

    /// Closes the direction without the accounting, when the connection is gone.
    pub fn abort(&mut self, updates: &AtomicWaker) {
        let _ = self.close_reading_side(updates);
    }

    fn close_reading_side(&mut self, updates: &AtomicWaker) -> Result<(), StreamError> {
        match &mut self.state {
            OutboundState::Open { writer, .. } | OutboundState::HalfClosed { writer, .. } => {
                writer.wake();
                updates.wake();
                self.state = OutboundState::Closed {
                    fin_state: FinState {
                        sent: false,
                        received: true,
                    },
                };
                Ok(())
            },
            OutboundState::Closed { fin_state } if fin_state.received => {
                Err(StreamError("sent duplicate FIN_READ"))
            },
            OutboundState::Closed { fin_state } => {
                fin_state.received = true;
                Ok(())
            },
        }
    }

    #[cfg(test)]
    pub fn unacked(&self) -> u64 {
        self.ledger.window.unacked()
    }

    /// Returns the advance the stream drew and has not repaid.
    #[cfg(test)]
    pub fn advance(&self) -> u64 {
        self.ledger.loan.advance()
    }

    /// Gives the stream an allowance of the given size, to decay from now.
    #[cfg(test)]
    pub fn prime(&mut self, allowance: u32, srtt: f64, now: Instant) {
        self.ledger
            .rate
            .grant_allowance(f64::from(allowance), now, srtt);
    }
}

/// What the sender knows about the bytes of a stream in flight, and how it decides what to send.
struct Ledger {
    /// Bytes in flight, against the base window the peer grants to the stream and the peer's pool.
    window: Window,
    /// Largest `DATA` payload.
    frame_limit: u64,
    /// What the stream owes to the advance of the connection.
    loan: Loan,
    /// The rate at which the reader of the peer proved to read, `R_s`.
    rate: RateEstimate,
}

impl Ledger {
    /// Decides how many of the `waiting` bytes the stream sends now, and accounts for them.
    ///
    /// With `base_room = max(0, B - unacked)` and `full = min(frame_limit, waiting)`:
    ///
    /// 1. Nothing waits: nothing to send.
    /// 2. `full` fits in the base window: send it, no turn needed.
    /// 3. Otherwise the stream may use `shared` bytes of the pool besides its base window:
    ///    what its borrowing rule still permits and the pool has room for, but only if it is its turn.
    ///    It sends `n = min(full, base_room + shared)` bytes if that is any.
    ///    If it used the pool, it gives the turn away, and takes a new one if it has more to send.
    ///    - Nothing can be sent because the borrowing rule permits nothing: only the stream's own credit
    ///      can help, so it waits for that outside of the turn queue. Nothing wakes it when other streams
    ///      repay the budget of the advance (see "No waking on freed budget" in `TRANSIT_CONTROL.md`).
    ///    - Nothing can be sent otherwise (not its turn, or the pool is empty): it waits in the turn queue.
    ///
    /// The borrowing rule is `proven + unproven`: the allowance grown by the advance the stream holds,
    /// and the part of the grant that the budget still covers (see `TRANSIT_CONTROL.md`).
    /// What the stream takes from the pool beyond `proven` is drawn from the advance.
    ///
    /// A frame is whatever fits. Waiting for a whole frame would let residue of credit, a small
    /// pool or a small window block a stream that has room.
    fn plan(&mut self, ctx: &mut Ctx<'_>, waiting: usize) -> Option<usize> {
        let waiting = waiting as u64;
        if waiting == 0 {
            ctx.flow.pass_turn(ctx.me);
            return None;
        }

        // An idle stream with something to send starts an episode. `unacked < T_c` leaves no advance behind:
        // the advance is part of the bytes in flight beyond `B`, and credit pays it back first.
        if self.window.is_idle() {
            self.loan.start_episode(&ctx.flow.advance);
        }

        let base_room = self.window.base_room();
        let full = self.frame_limit.min(waiting);
        if base_room >= full {
            self.window.add(&mut ctx.flow.tx_pool, full);
            return Some(to_len(full));
        }

        let allowance = ctx
            .flow
            .srtt
            .secs()
            .map_or(0.0, |srtt| self.rate.allowance(ctx.now, srtt));
        // Whole bytes of the allowance, and of the advance in hand, that are not borrowed yet.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let proven = (allowance + self.loan.advance() as f64 - self.window.borrowed() as f64)
            .max(0.0) as u64;
        let may_borrow = proven + self.loan.unproven(&ctx.flow.advance);
        let shared = if ctx.flow.turns.has_turn(ctx.me) {
            may_borrow.min(ctx.flow.tx_pool.room())
        } else {
            0
        };
        let n = full.min(base_room + shared);
        if n == 0 {
            if may_borrow == 0 {
                ctx.flow.pass_turn(ctx.me);
            } else {
                ctx.flow.turns.join(ctx.me);
            }
            return None;
        }

        let taken = n.saturating_sub(base_room);
        self.window.add(&mut ctx.flow.tx_pool, n);
        self.loan
            .draw(&mut ctx.flow.advance, taken.saturating_sub(proven));
        if n > base_room {
            ctx.flow.turns.leave(ctx.me);
            if waiting > n {
                ctx.flow.turns.join(ctx.me);
            }
            if ctx.flow.tx_pool.has_room() {
                ctx.flow.turns.wake_front();
            }
        }
        Some(to_len(n))
    }
}

/// Converts a number of bytes to send, which is never more than the bytes waiting.
fn to_len(bytes: u64) -> usize {
    usize::try_from(bytes).expect("never more than the bytes waiting")
}
