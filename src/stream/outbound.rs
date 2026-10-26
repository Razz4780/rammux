use std::{
    io,
    num::NonZeroU16,
    ops::Not,
    task::{Poll, Waker},
};

use bytes::Bytes;
use tokio::time::Instant;

use crate::{
    error::StreamError,
    global_pool::GlobalPool,
    stream::{FinState, StreamRef, waker::WakerSlot},
    transit::{self, RateEstimate},
};

/// What the sending direction of a stream needs from the rest of the connection.
pub struct Ctx<'a> {
    /// State of the connection.
    pub global: &'a mut GlobalPool,
    /// This stream, as the turn queue knows it.
    pub me: &'a StreamRef,
    /// Poller of this stream. The caller holds the lock of the stream.
    pub poller: &'a mut WakerSlot,
    /// The time.
    pub now: Instant,
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

impl OutboundTraffic {
    pub fn new(frame_limit: NonZeroU16, base_window: u32) -> Self {
        Self {
            state: OutboundState::Open {
                ready_data: Default::default(),
                writer: Default::default(),
            },
            ledger: Ledger {
                base_window: u64::from(base_window),
                frame_limit: u64::from(frame_limit.get()),
                unacked: 0,
                rate: RateEstimate::new(Instant::now()),
                queued: false,
            },
        }
    }

    /// Polls the next outbound traffic update.
    ///
    /// Returns ready outbound frame data and `FIN_WRITE`.
    ///
    /// Notes:
    /// 1. If `FIN_WRITE` has already been sent, this method will return `([], false)`.
    /// 2. This method is not responsible for registering [`Waker`] for updates.
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
                let fin = if ready_data.is_empty() {
                    writer.wake();
                    self.state = OutboundState::Closed {
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
            } => {
                // A closed stream must not hold up the others.
                self.ledger.pass_turn(ctx);
                Poll::Pending
            },

            OutboundState::Closed { fin_state } => {
                self.ledger.pass_turn(ctx);
                fin_state.sent = true;
                Poll::Ready((Default::default(), true))
            },
        }
    }

    pub fn close_writing(&mut self, updates_poller: &mut WakerSlot) {
        let OutboundState::Open { ready_data, writer } = &mut self.state else {
            return;
        };
        let new_state = if ready_data.is_empty() {
            updates_poller.wake();
            OutboundState::Closed {
                fin_state: FinState {
                    sent: false,
                    received: false,
                },
            }
        } else {
            OutboundState::HalfClosed {
                ready_data: std::mem::take(ready_data),
                writer: std::mem::take(writer),
            }
        };
        self.state = new_state;
    }

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

    pub fn poll_flushed(&mut self, waker: &Waker) -> Poll<()> {
        match &mut self.state {
            OutboundState::Open { ready_data, writer } => {
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
        let OutboundState::Open { ready_data, .. } = &mut self.state else {
            return Err(io::ErrorKind::BrokenPipe.into());
        };
        if ready_data.is_empty().not() {
            return Err(io::Error::other("pipe not ready"));
        }
        *ready_data = data;
        if ready_data.is_empty().not() {
            updates_poller.wake();
        }
        Ok(())
    }

    /// The peer stopped reading the stream. Its counter of bytes in flight is zeroed,
    /// which returns the share of the pool that the stream used.
    pub fn received_fin_read(&mut self, ctx: &mut Ctx<'_>) -> Result<(), StreamError> {
        let result = self.close_reading_side(ctx.poller);
        if result.is_ok() {
            let freed = self.ledger.set_unacked(ctx.global, 0);
            if freed {
                ctx.global.wake_front(Some((ctx.me, &mut *ctx.poller)));
            }
        }
        result
    }

    /// Closes the stream without the accounting, when the connection is dropped.
    pub fn abort(&mut self, updates_poller: &mut WakerSlot) {
        let _ = self.close_reading_side(updates_poller);
    }

    fn close_reading_side(&mut self, updates_poller: &mut WakerSlot) -> Result<(), StreamError> {
        match &mut self.state {
            OutboundState::Open { writer, .. } | OutboundState::HalfClosed { writer, .. } => {
                writer.wake();
                updates_poller.wake();
                self.state = OutboundState::Closed {
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

    /// The peer returned credit. The bytes go back to the shared pool, not to this stream.
    pub fn received_window_update(
        &mut self,
        credit: u32,
        ctx: &mut Ctx<'_>,
    ) -> Result<(), StreamError> {
        if credit == 0 {
            return Ok(());
        }
        if let OutboundState::Closed {
            fin_state: FinState { received: true, .. },
        } = &self.state
        {
            return Err(StreamError("sent window update after FIN_READ"));
        }
        let credit = u64::from(credit);
        let unacked = self
            .ledger
            .unacked
            .checked_sub(credit)
            .ok_or(StreamError("returned more credit than was in flight"))?;

        let freed = self.ledger.set_unacked(ctx.global, unacked);
        // The estimate only runs once the loop time is known, or its time constant is not.
        if let Some(srtt) = ctx.global.loop_secs() {
            self.ledger.rate.add(
                u32::try_from(credit).expect("the credit is a u32"),
                ctx.now,
                srtt,
            );
        }
        if freed {
            ctx.global.wake_front(Some((ctx.me, &mut *ctx.poller)));
        }
        // A larger allowance, and the stream may have been waiting for it.
        match &self.state {
            OutboundState::Open { ready_data, .. }
            | OutboundState::HalfClosed { ready_data, .. }
                if ready_data.is_empty().not() =>
            {
                ctx.poller.wake()
            },
            _ => {},
        }
        Ok(())
    }

    pub fn fin_state(&self) -> FinState {
        match &self.state {
            OutboundState::Open { .. } | OutboundState::HalfClosed { .. } => FinState::default(),
            OutboundState::Closed { fin_state, .. } => *fin_state,
        }
    }

    /// Takes the stream out of the turn queue. The stream is gone.
    pub fn leave_turns(&mut self, global: &mut GlobalPool, me: &StreamRef) {
        if self.ledger.leave_turns(global, me) && global.room_left(Instant::now()) {
            global.wake_front(None);
        }
    }

    #[cfg(test)]
    pub fn unacked(&self) -> u64 {
        self.ledger.unacked
    }

    /// Gives the stream an allowance of the given size, to decay from now.
    #[cfg(test)]
    pub fn prime(&mut self, allowance: u32, srtt: f64, now: Instant) {
        self.ledger
            .rate
            .grant_allowance(f64::from(allowance), now, srtt);
    }

    #[cfg(test)]
    pub fn is_queued(&self) -> bool {
        self.ledger.queued
    }
}

/// Converts a number of bytes to send, which is never more than the bytes waiting.
fn len(bytes: u64) -> usize {
    usize::try_from(bytes).expect("never more than the bytes waiting")
}

/// What the sender knows about the bytes of a stream in flight, and how it decides what to send.
struct Ledger {
    /// The base window `B` the peer grants to the stream.
    base_window: u64,
    /// Largest `DATA` payload.
    frame_limit: u64,
    /// Bytes sent and not credited back.
    ///
    /// The part beyond `base_window` is borrowed from the peer's pool.
    unacked: u64,
    /// The rate at which the reader of the peer proved to read, `R_s`.
    rate: RateEstimate,
    /// Whether the stream is in the turn queue.
    queued: bool,
}

impl Ledger {
    /// Decides how many of the `waiting` bytes the stream sends now, and accounts for them.
    ///
    /// This is the polling order of the sending direction. In short, with
    /// `base_room = max(0, B - unacked)` and `full = min(frame_limit, waiting)`:
    ///
    /// 1. Nothing waits: nothing to send.
    /// 2. `full` fits in the base window: send it, no turn needed.
    /// 3. Otherwise the stream may use `shared` bytes of the pool besides its base window:
    ///    what its allowance still permits and the pool has room for, but only if it is its turn.
    ///    It sends `n = min(full, base_room + shared)` bytes if that is any.
    ///    If it used the pool, it gives the turn away, and takes a new one if it has more to send.
    ///    - Nothing can be sent because the allowance is used up: only the stream's own credit can help,
    ///      so it waits for that, outside of the queue.
    ///    - Nothing can be sent otherwise (not its turn, or the pool is empty): it waits in the queue.
    ///
    /// A frame is whatever fits. Waiting for a whole frame would let residue of credit, a small
    /// pool or a small window block a stream that has room.
    fn plan(&mut self, ctx: &mut Ctx<'_>, waiting: usize) -> Option<usize> {
        let waiting = waiting as u64;
        if waiting == 0 {
            self.pass_turn(ctx);
            return None;
        }

        let base_room = self.base_window.saturating_sub(self.unacked);
        let full = self.frame_limit.min(waiting);
        let has_turn = ctx.global.has_turn(ctx.me);
        // The connection window of DelayControl applies to every byte, and needs the turn.
        let window_room = ctx.global.window_room(ctx.now);
        let conn_room = match window_room {
            None => u64::MAX,
            Some(room) if has_turn => room,
            Some(_) => 0,
        };

        if base_room >= full {
            // A frame within the base window does not wait for the turn: the turns share the window
            // between streams that borrow, and a stream within its base window is a sparse flow
            // (fq_codel's idea), at most a base window per round trip. If the window has no room,
            // it waits at the front of the queue. The window never splits a frame (see `fits`).
            let n = fits(full, window_room.unwrap_or(u64::MAX));
            if n == 0 {
                self.note_window_limit(ctx, window_room, full);
                self.join_turns_front(ctx);
                return None;
            }
            self.note_window_limit(ctx, window_room, full);
            self.sent(ctx.global, n);
            if window_room.is_some() && self.queued {
                self.after_turn(ctx, waiting, n);
            }
            return Some(len(n));
        }

        let borrowed = transit::borrowed(self.unacked, self.base_window);
        // The estimate averages over a loop, the allowance covers `k` of them.
        let allowance = ctx
            .global
            .loop_secs()
            .zip(ctx.global.tau())
            .map_or(0.0, |(srtt, tau)| self.rate.value(ctx.now, srtt) * tau);
        // Whole bytes of the allowance that are not borrowed yet.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let borrow_allow = (allowance - borrowed as f64).max(0.0) as u64;
        let shared = if has_turn {
            borrow_allow.min(ctx.global.pool_room())
        } else {
            0
        };
        // What the stream could send, if not for the connection window.
        let wanted = full.min(base_room + shared);
        let n = fits(wanted, conn_room);
        self.note_window_limit(ctx, window_room, wanted);
        if n == 0 {
            if wanted > 0 {
                // The window or the turn is what stops it.
                self.join_turns(ctx);
            } else if borrow_allow == 0 {
                self.pass_turn(ctx);
            } else {
                self.join_turns(ctx);
            }
            return None;
        }

        self.sent(ctx.global, n);
        if n > base_room || (window_room.is_some() && self.queued) {
            self.after_turn(ctx, waiting, n);
        }
        Some(len(n))
    }

    /// The stream sent `n` of its `waiting` bytes in its turn: it gives the turn away,
    /// takes another one if it has more to send, and wakes the next stream while there is room.
    fn after_turn(&mut self, ctx: &mut Ctx<'_>, waiting: u64, n: u64) {
        self.leave_turns(ctx.global, ctx.me);
        if waiting > n {
            self.join_turns(ctx);
        }
        if ctx.global.room_left(ctx.now) {
            ctx.global.wake_front(Some((ctx.me, &mut *ctx.poller)));
        }
    }

    /// Tells the window that it kept `wanted` bytes from being sent, if it did.
    fn note_window_limit(&self, ctx: &mut Ctx<'_>, window_room: Option<u64>, wanted: u64) {
        if window_room.is_some_and(|room| room < wanted) {
            ctx.global.note_window_limit(ctx.now, wanted);
        }
    }

    /// Gives the turn away, if the stream waits for it.
    fn pass_turn(&mut self, ctx: &mut Ctx<'_>) {
        if self.leave_turns(ctx.global, ctx.me) && ctx.global.room_left(ctx.now) {
            ctx.global.wake_front(Some((ctx.me, &mut *ctx.poller)));
        }
    }

    /// Puts the stream at the front of the queue, or moves it there.
    fn join_turns_front(&mut self, ctx: &mut Ctx<'_>) {
        if self.queued {
            ctx.global.leave_turns(ctx.me);
        }
        self.queued = true;
        ctx.global.join_turns_front(ctx.me);
    }

    fn join_turns(&mut self, ctx: &mut Ctx<'_>) {
        if self.queued {
            return;
        }
        self.queued = true;
        ctx.global.join_turns(ctx.me);
    }

    /// Returns whether the stream was in the queue.
    fn leave_turns(&mut self, global: &mut GlobalPool, me: &StreamRef) -> bool {
        let was_queued = std::mem::take(&mut self.queued);
        if was_queued {
            global.leave_turns(me);
        }
        was_queued
    }

    fn sent(&mut self, global: &mut GlobalPool, len: u64) {
        let unacked = self.unacked + len;
        self.set_unacked(global, unacked);
    }

    /// Sets the counter of bytes in flight, and keeps the usage of the pool in sync.
    ///
    /// Returns whether the stream gave some of the pool back.
    fn set_unacked(&mut self, global: &mut GlobalPool, unacked: u64) -> bool {
        let old = transit::borrowed(self.unacked, self.base_window);
        let new = transit::borrowed(unacked, self.base_window);
        transit::rebalance(&mut global.tx_used, old, new);
        self.unacked = unacked;
        new < old
    }
}

enum OutboundState {
    /// Traffic direction is fully open.
    Open {
        ready_data: Bytes,
        writer: WakerSlot,
    },
    /// Local writer closed, but there's still data to be flushed.
    HalfClosed {
        /// Never empty.
        ready_data: Bytes,
        writer: WakerSlot,
    },
    /// One of the sides closed and there's no pending data.
    Closed { fin_state: FinState },
}

/// What the connection window lets a stream send of the `wanted` bytes: all of them, or nothing.
///
/// Silly window avoidance (RFC 813, RFC 1122 4.2.3.4). Room in the window opens a few bytes at a
/// time, with every delivery report. A sender that fills every opening sends a frame per report,
/// and with a `PING` on every frame that is blocked (`TRANSIT_CONTROL.md`) also a report per
/// frame: frames of a few bytes, hundreds of thousands per second, whose headers and reports
/// fill the link. Waiting for the whole frame cannot block for good: `wanted` is at most
/// `frame_limit`, and so is the floor of the window, so a drained window always has room for it.
fn fits(wanted: u64, conn_room: u64) -> u64 {
    if conn_room >= wanted { wanted } else { 0 }
}
