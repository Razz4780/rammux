//! Tests of the sending rules and the turn queue, with several streams in one selector.
//!
//! The streams have the base window of 16 bytes and frames of 8 bytes.
//! The loop time is 1 s, so the estimates have the time constant of 1 s and the allowances cover 2 s (the paused clock does not move
//! unless a test moves it).

use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use async_selector::selector::Selector;
use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt, task::ArcWake};
use tokio::time::Instant;

use crate::{
    StreamId,
    buffer::Data,
    config::RammuxConfig,
    flow::{Advance, Srtt},
    stream::{ConnFlow, RammuxDuplex, StreamRef, handle::StreamHandle, updates::StreamUpdates},
    wire::header::ControlFlags,
};

const CONFIG: RammuxConfig = RammuxConfig {
    frame_limit: NonZeroU16::new(8).unwrap(),
    max_inbound_streams: 8,
    max_outbound_streams: 8,
    local_recv_window: NonZeroU32::new(16).unwrap(),
    remote_recv_window: NonZeroU32::new(16).unwrap(),
    local_recv_pool: 1 << 20,
    remote_recv_pool: 8,
    advance_grant: 0.0,
    advance_budget: 0.0,
    ping_bytes: NonZeroU32::new(32).unwrap(),
    ping_interval: Duration::from_secs(1),
    inbound_frame_timeout: Duration::from_secs(2),
};

/// The loop time is 1 s.
const SRTT: f64 = 1.0;

struct Woken(AtomicBool);

impl ArcWake for Woken {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::SeqCst);
    }
}

struct Strm {
    n: u16,
    id: StreamId,
    handle: StreamHandle,
    duplex: RammuxDuplex,
    me: StreamRef,
}

struct Bed {
    selector: Selector<StreamUpdates, ConnFlow>,
    streams: Vec<Strm>,
}

impl Bed {
    /// Creates a bed where the peer's pool is `tx_pool` bytes.
    fn new(tx_pool: u32) -> Self {
        let mut flow = ConnFlow::new(1 << 20, tx_pool);
        flow.srtt = Srtt::of(Duration::from_secs(1));
        Self {
            selector: Selector::new(flow),
            streams: Vec::new(),
        }
    }

    /// Adds a stream, returns its index.
    fn add(&mut self) -> usize {
        let n = u16::try_from(self.streams.len()).unwrap();
        self.add_with_id(StreamId::from_be_bytes((n * 2).to_be_bytes()))
    }

    /// Adds a stream with the ID of a stream that was closed.
    fn add_reusing(&mut self, closed: usize) -> usize {
        self.add_with_id(self.streams[closed].id)
    }

    fn add_with_id(&mut self, id: StreamId) -> usize {
        let n = u16::try_from(self.streams.len()).unwrap();
        let (handle, updates, duplex) = crate::stream::new(id, true, &CONFIG);
        self.selector.push(updates);
        self.streams.push(Strm {
            n,
            id,
            me: handle.0.clone(),
            handle,
            duplex,
        });
        usize::from(n)
    }

    fn flow(&self) -> &ConnFlow {
        self.selector.strategy()
    }

    /// Gives the stream an allowance of `bytes`, as if the peer's reader had just read that much.
    fn prime(&self, s: usize, bytes: u32) {
        let mut state = self.streams[s].me.lock();
        state.outbound.prime(bytes, SRTT, Instant::now());
    }

    /// The application writes to the stream.
    fn write(&mut self, s: usize, len: usize) {
        self.streams[s]
            .duplex
            .feed(Bytes::from(vec![7; len]))
            .now_or_never()
            .expect("the stream is ready")
            .unwrap();
    }

    /// The peer returns credit.
    fn credit(&mut self, s: usize, credit: u32) {
        let strm = &mut self.streams[s];
        strm.handle
            .received(
                NonZeroU32::new(credit),
                Data::default(),
                fins(false, false),
                self.selector.strategy_mut(),
            )
            .unwrap();
    }

    /// Polls until the selector has nothing more, and nobody woke it up.
    /// Returns the (stream, data length) of the updates.
    fn drain(&mut self) -> Vec<(usize, usize)> {
        let flag = Arc::new(Woken(AtomicBool::new(false)));
        let waker = futures::task::waker(flag.clone());
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            flag.0.store(false, Ordering::SeqCst);
            match self.selector.poll_next_unpin(&mut cx) {
                Poll::Ready(Some((update, _))) => {
                    let n = self
                        .streams
                        .iter()
                        .rfind(|s| s.id == update.id)
                        .map(|s| usize::from(s.n))
                        .unwrap();
                    if !update.data.is_empty() {
                        out.push((n, update.data.len()));
                    }
                },
                Poll::Ready(None) => break,
                Poll::Pending if flag.0.load(Ordering::SeqCst) => {},
                Poll::Pending => break,
            }
        }
        out
    }

    /// Like [`Self::drain`], grouped by stream: the interleaving of the streams is not what is tested.
    fn drain_grouped(&mut self) -> Vec<(usize, usize)> {
        let mut out = self.drain();
        out.sort_by_key(|(s, _)| *s);
        out
    }

    fn queue(&self) -> Vec<usize> {
        self.flow()
            .turns
            .iter()
            .map(|queued| {
                self.streams
                    .iter()
                    .position(|s| std::ptr::eq(Arc::as_ptr(&s.me), queued))
                    .expect("a queued stream is a stream of the bed")
            })
            .collect()
    }

    fn is_queued(&self, s: usize) -> bool {
        self.flow().turns.contains(&self.streams[s].me)
    }
}

/// The guard of the rule "free room never lets a stream skip the queue": it fails (and it is the only test
/// that does) if `ConnFlow::has_turn` is forced to true. The equal-share sim tests do not tell,
/// because the front is woken before the stream that got the credit, and no scenario with equal writes
/// that was tried (staggered starts, throttled readers, flushed or not) separates them.
#[tokio::test(start_paused = true)]
async fn a_stream_woken_by_its_own_credit_does_not_take_free_room_while_others_wait() {
    let mut bed = Bed::new(8);
    let [x, y, z] = [bed.add(), bed.add(), bed.add()];
    for s in [x, y, z] {
        bed.prime(s, 1000);
    }

    // X takes the whole pool, and waits for more.
    bed.write(x, 32);
    assert_eq!(bed.drain(), [(x, 8), (x, 8), (x, 8)]);
    assert_eq!(bed.flow().tx_pool.used(), 8);
    assert_eq!(bed.queue(), [x]);
    // Y and Z use their base windows and queue up behind X.
    bed.write(y, 32);
    bed.write(z, 32);
    assert_eq!(bed.drain_grouped(), [(y, 8), (y, 8), (z, 8), (z, 8)]);
    assert_eq!(bed.queue(), [x, y, z]);

    // Y gets a little credit, which is not enough for a frame, and then X returns what it borrowed.
    bed.credit(y, 3);
    bed.credit(x, 8);
    assert_eq!(bed.flow().tx_pool.used(), 0, "the pool is free");
    // Y is woken first, but it is not its turn: it only uses its base window.
    // Then X takes the room.
    assert_eq!(bed.drain(), [(y, 3), (x, 8)]);
    assert_eq!(bed.flow().tx_pool.used(), 8);
    // Y keeps its place in the queue, and X is done.
    assert_eq!(bed.queue(), [y, z]);
}

#[tokio::test(start_paused = true)]
async fn the_front_without_allowance_passes_the_turn() {
    let mut bed = Bed::new(8);
    let [w, x, y] = [bed.add(), bed.add(), bed.add()];
    bed.prime(w, 1000);
    bed.prime(x, 10);
    bed.prime(y, 1000);

    // W takes the pool and has nothing more to send.
    bed.write(w, 24);
    assert_eq!(bed.drain(), [(w, 8), (w, 8), (w, 8)]);
    assert!(bed.queue().is_empty());
    // X and Y wait for room, X first.
    bed.write(x, 32);
    bed.write(y, 32);
    assert_eq!(bed.drain_grouped(), [(x, 8), (x, 8), (y, 8), (y, 8)]);
    assert_eq!(bed.queue(), [x, y]);

    // The allowance of X fades away (10 * e^-5 < 1 byte), the one of Y is renewed.
    tokio::time::advance(Duration::from_secs(10)).await;
    bed.prime(y, 1000);
    // W returns the pool and the front is woken. It cannot use the room, so Y gets it.
    bed.credit(w, 8);
    assert_eq!(bed.drain(), [(y, 8)]);
    assert!(
        !bed.is_queued(x),
        "X waits for its own credit, outside of the queue"
    );
    assert_eq!(bed.queue(), [y]);

    // When X gets credit it needs shared room again; there is none and it takes a place in the queue.
    bed.prime(x, 1000);
    bed.credit(x, 4);
    // 4 bytes of the base window go first.
    assert_eq!(bed.drain(), [(x, 4)]);
    assert!(bed.is_queued(x));
    assert_eq!(bed.queue(), [y, x]);
}

#[tokio::test(start_paused = true)]
async fn a_slow_reader_uses_the_pool_in_proportion_to_its_rate() {
    let mut bed = Bed::new(8);
    let [fast, slow] = [bed.add(), bed.add()];
    bed.prime(fast, 1000);
    // The slow reader has proven nothing.
    bed.write(fast, 32);
    bed.write(slow, 64);
    // Fast: base window, the pool, and queue for more. Slow: its base window and that is it.
    assert_eq!(
        bed.drain_grouped(),
        [(fast, 8), (fast, 8), (fast, 8), (slow, 8), (slow, 8)]
    );
    assert_eq!(bed.queue(), [fast]);
    assert!(!bed.is_queued(slow), "it waits for credit, not for room");

    // A trickle of credit, a byte per second, buys a byte or two of the pool at most, in tiny frames.
    for _ in 0..4 {
        bed.credit(slow, 1);
        let sent = bed.drain();
        assert!(sent.iter().all(|(s, n)| *s == slow && *n <= 2), "{sent:?}");
        let unacked = bed.streams[slow].me.lock().outbound.unacked();
        assert!(unacked <= 16 + 2, "{unacked}");
        tokio::time::advance(Duration::from_secs(1)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_small_allowance_or_pool_is_used_in_smaller_frames() {
    let mut bed = Bed::new(8);
    let x = bed.add();

    // The allowance of X covers 5 bytes of the 8 that make a frame: it sends 5.
    bed.prime(x, 5);
    bed.write(x, 32);
    assert_eq!(bed.drain(), [(x, 8), (x, 8), (x, 5)]);
    assert_eq!(bed.flow().tx_pool.used(), 5);
    // And then waits for its own credit, outside of the queue.
    assert!(!bed.is_queued(x));

    // The pool has 5 bytes of room for the 8 that Y wants: the frame is 5 bytes.
    let mut bed = Bed::new(5);
    let y = bed.add();
    bed.prime(y, 1000);
    bed.write(y, 32);
    assert_eq!(bed.drain(), [(y, 8), (y, 8), (y, 5)]);
    assert_eq!(bed.flow().tx_pool.used(), 5);
    assert_eq!(bed.queue(), [y], "Y waits at the front for room");

    // A pool and a base window smaller than a frame still work.
    let mut bed = Bed::new(3);
    let z = bed.add();
    bed.prime(z, 1000);
    bed.write(z, 40);
    let sent = bed.drain();
    assert_eq!(sent.iter().map(|(_, n)| n).sum::<usize>(), 16 + 3);
}

#[tokio::test(start_paused = true)]
async fn a_message_shorter_than_a_frame_is_sent_whole_from_shared_room() {
    let mut bed = Bed::new(8);
    let x = bed.add();
    bed.prime(x, 1000);
    bed.write(x, 16);
    assert_eq!(bed.drain(), [(x, 8), (x, 8)]);
    bed.credit(x, 3);
    assert_eq!(bed.drain(), Vec::new());
    // Base room is 3, the message is 5 bytes: it is sent whole, 2 bytes from the pool.
    bed.write(x, 5);
    assert_eq!(bed.drain(), [(x, 5)]);
    assert_eq!(bed.flow().tx_pool.used(), 2);
}

#[tokio::test(start_paused = true)]
async fn no_stall_when_the_base_room_is_below_a_frame_and_the_pool_is_exhausted() {
    let mut bed = Bed::new(8);
    let [x, y] = [bed.add(), bed.add()];
    bed.prime(x, 1000);
    bed.prime(y, 1000);
    bed.write(x, 40);
    assert_eq!(bed.drain().len(), 3);
    // The pool is exhausted, X is at the front. Y is left with 5 bytes of the base window.
    bed.write(y, 21);
    assert_eq!(bed.drain(), [(y, 8), (y, 8)]);
    bed.credit(y, 5);
    assert_eq!(
        bed.drain(),
        [(y, 5)],
        "the residue of the base window is not stranded"
    );
}

#[tokio::test(start_paused = true)]
async fn a_queued_stream_that_closes_leaves_the_queue() {
    let mut bed = Bed::new(8);
    let [x, y, z] = [bed.add(), bed.add(), bed.add()];
    for s in [x, y, z] {
        bed.prime(s, 1000);
    }
    bed.write(x, 24);
    bed.write(y, 32);
    bed.write(z, 32);
    bed.drain();
    // X holds the pool, Y and Z wait.
    assert_eq!(bed.queue(), [y, z]);

    // The peer of Y stops reading. Y has to leave the queue.
    bed.streams[y]
        .handle
        .received(
            None,
            Data::default(),
            fins(false, true),
            bed.selector.strategy_mut(),
        )
        .unwrap();
    bed.drain();
    assert_eq!(bed.queue(), [z]);
    assert!(!bed.is_queued(y));

    // Z gets the room X frees.
    bed.credit(x, 8);
    assert_eq!(bed.drain(), [(z, 8)]);
    assert_eq!(bed.flow().tx_pool.used(), 8);
}

#[tokio::test(start_paused = true)]
async fn fin_read_returns_the_pool_and_wakes_the_queue() {
    let mut bed = Bed::new(8);
    let [x, y] = [bed.add(), bed.add()];
    bed.prime(x, 1000);
    bed.prime(y, 1000);
    bed.write(x, 24);
    bed.write(y, 32);
    bed.drain();
    assert_eq!(bed.queue(), [y]);
    assert_eq!(bed.flow().tx_pool.used(), 8);

    // The reader of X is gone: the pool is free without any credit.
    bed.streams[x]
        .handle
        .received(
            None,
            Data::default(),
            fins(false, true),
            bed.selector.strategy_mut(),
        )
        .unwrap();
    assert_eq!(bed.flow().tx_pool.used(), 0);
    assert_eq!(bed.drain(), [(y, 8)]);
}

#[tokio::test(start_paused = true)]
async fn credit_larger_than_the_data_in_flight_is_a_violation() {
    let mut bed = Bed::new(8);
    let x = bed.add();
    bed.write(x, 8);
    bed.drain();
    let strm = &mut bed.streams[x];
    strm.handle
        .received(
            NonZeroU32::new(9),
            Data::default(),
            fins(false, false),
            bed.selector.strategy_mut(),
        )
        .unwrap_err();
}

#[tokio::test(start_paused = true)]
async fn the_pool_is_never_overdrawn() {
    // Many streams, a little room: the sum of what they borrow never exceeds the pool.
    let mut bed = Bed::new(24);
    let streams = (0..6).map(|_| bed.add()).collect::<Vec<_>>();
    for &s in &streams {
        bed.prime(s, 1000);
        bed.write(s, 64);
    }
    bed.drain();
    assert_eq!(bed.flow().tx_pool.used(), 24);
    assert_eq!(bed.flow().turns.len(), 6);
    for round in 0..50 {
        let s = streams[round % streams.len()];
        let unacked = bed.streams[s].me.lock().outbound.unacked();
        if unacked > 0 {
            bed.credit(s, u32::try_from(unacked.min(8)).unwrap());
        }
        bed.drain();
        assert!(bed.flow().tx_pool.used() <= 24);
        let borrowed = streams
            .iter()
            .map(|&s| {
                bed.streams[s]
                    .me
                    .lock()
                    .outbound
                    .unacked()
                    .saturating_sub(16)
            })
            .sum::<u64>();
        assert_eq!(borrowed, bed.flow().tx_pool.used());
    }
}

#[tokio::test(start_paused = true)]
async fn a_reused_stream_id_does_not_confuse_the_queue() {
    let mut bed = Bed::new(8);
    let [holder, old] = [bed.add(), bed.add()];
    bed.prime(holder, 1000);
    bed.prime(old, 1000);
    bed.write(holder, 24);
    bed.write(old, 32);
    bed.drain();
    assert_eq!(bed.queue(), [old]);

    // The old stream closes completely: the peer stops reading and writing, so both of its FINs go out.
    bed.streams[old]
        .handle
        .received(
            None,
            Data::default(),
            fins(true, true),
            bed.selector.strategy_mut(),
        )
        .unwrap();
    bed.drain();
    assert_eq!(bed.flow().turns.len(), 0);

    // Another stream gets the ID, and waits for the pool.
    let new = bed.add_reusing(old);
    bed.prime(new, 1000);
    bed.write(new, 32);
    bed.drain();
    assert_eq!(bed.queue(), [new]);

    // The room goes to the new stream.
    bed.credit(holder, 8);
    assert_eq!(bed.drain(), [(new, 8)]);
}

#[tokio::test(start_paused = true)]
async fn a_base_window_smaller_than_a_frame_works() {
    let config = RammuxConfig {
        remote_recv_window: NonZeroU32::new(5).unwrap(),
        ..CONFIG
    };
    let mut flow = ConnFlow::new(1 << 20, 0);
    flow.srtt = Srtt::of(Duration::from_secs(1));
    let mut selector = Selector::new(flow);
    let (handle, updates, mut duplex) =
        crate::stream::new(StreamId::from_be_bytes([0, 0]), true, &config);
    selector.push(updates);
    duplex
        .feed(Bytes::from(vec![1; 20]))
        .now_or_never()
        .unwrap()
        .unwrap();
    let (update, _) = selector.next().await.unwrap();
    assert_eq!(update.data.len(), 5);
    assert!(selector.next().now_or_never().is_none());
    handle
        .received(
            NonZeroU32::new(5),
            Data::default(),
            fins(false, false),
            selector.strategy_mut(),
        )
        .unwrap();
    let (update, _) = selector.next().await.unwrap();
    assert_eq!(update.data.len(), 5);
}

fn fins(fin_write: bool, fin_read: bool) -> ControlFlags {
    ControlFlags {
        syn: false,
        fin_read,
        fin_write,
    }
}

// The advance. The pool is 64 bytes, a stream draws 16 at most per episode (g = 1/4)
// and all streams together 32 at most (G = 1/2). `T_c` is 8 bytes.

const POOL: u32 = 64;

impl Bed {
    /// Creates a bed with the advance of `grant` and `budget` fractions of the pool.
    fn with_advance(grant: f32, budget: f32) -> Self {
        let mut bed = Self::new(POOL);
        bed.selector.strategy_mut().advance = Advance::new(POOL, grant, budget);
        bed
    }

    /// The sum of the advances, `V`.
    fn v(&self) -> u64 {
        self.flow().advance.used()
    }

    fn advance(&self, s: usize) -> u64 {
        self.streams[s].me.lock().outbound.advance()
    }

    fn unacked(&self, s: usize) -> u64 {
        self.streams[s].me.lock().outbound.unacked()
    }

    fn fin_read(&mut self, s: usize) {
        self.streams[s]
            .handle
            .received(
                None,
                Data::default(),
                fins(false, true),
                self.selector.strategy_mut(),
            )
            .unwrap();
    }

    fn waits_for_budget(&self, s: usize) -> bool {
        self.flow().budget_waiters.contains(&self.streams[s].me)
    }
}

#[tokio::test(start_paused = true)]
async fn an_idle_stream_draws_its_grant_and_no_more() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let x = bed.add();
    bed.write(x, 64);
    // The base window (16), then the grant (16): nothing is proven.
    assert_eq!(bed.drain(), [(x, 8), (x, 8), (x, 8), (x, 8)]);
    assert_eq!((bed.advance(x), bed.v()), (16, 16));
    assert_eq!(bed.flow().tx_pool.used(), 16);
    // The grant is spent and there is no new episode: the stream has 32 bytes in flight, not idle.
    assert!(bed.drain().is_empty());
    assert!(!bed.is_queued(x) && !bed.waits_for_budget(x));
}

#[tokio::test(start_paused = true)]
async fn without_the_advance_nothing_is_drawn() {
    let mut bed = Bed::new(POOL);
    let x = bed.add();
    bed.write(x, 64);
    assert_eq!(bed.drain(), [(x, 8), (x, 8)]);
    assert_eq!((bed.advance(x), bed.v()), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn credit_repays_the_advance_and_still_feeds_the_rate() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let x = bed.add();
    bed.write(x, 64);
    bed.drain();
    assert_eq!((bed.advance(x), bed.v()), (16, 16));

    bed.credit(x, 8);
    assert_eq!((bed.advance(x), bed.v()), (8, 8));
    // The credit proved 16 bytes (8 B/s * 2 s) and the 8 left of the advance count as proven too:
    // the stream may borrow 16 + 8 - 8 = 16 more. It is not the grant that sends them.
    let sent = bed.drain();
    assert_eq!(sent, [(x, 8), (x, 8)]);
    assert_eq!(
        (bed.advance(x), bed.v()),
        (8, 8),
        "proven bytes are not drawn"
    );

    bed.credit(x, 8);
    assert_eq!((bed.advance(x), bed.v()), (0, 0));
    // More credit than is owed leaves nothing negative.
    bed.credit(x, 8);
    assert_eq!((bed.advance(x), bed.v()), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_slow_reader_gets_one_grant() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let x = bed.add();
    bed.write(x, 64);
    bed.drain();
    // One byte of credit per second, the standing backlog stays above `T_c`.
    let mut v_max = bed.v();
    for _ in 0..8 {
        tokio::time::advance(Duration::from_secs(1)).await;
        bed.credit(x, 1);
        bed.drain();
        assert!(bed.unacked(x) >= 8, "never idle");
        v_max = v_max.max(bed.v());
        assert!(bed.advance(x) <= 16);
    }
    assert_eq!(v_max, 16, "no second grant");
    // The backlog is repaid at the read rate: 8 bytes of credit repaid 8 of the 16.
    assert_eq!(bed.advance(x), 8);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_caught_up_starts_a_new_episode() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let x = bed.add();
    bed.write(x, 32);
    bed.drain();
    bed.credit(x, 32);
    assert_eq!((bed.unacked(x), bed.advance(x), bed.v()), (0, 0, 0));
    // The allowance fades away, so it is the grant that sends the next burst.
    tokio::time::advance(Duration::from_secs(20)).await;
    bed.write(x, 64);
    assert_eq!(bed.drain().len(), 4);
    assert_eq!((bed.advance(x), bed.v()), (16, 16));
}

#[tokio::test(start_paused = true)]
async fn fin_read_returns_the_whole_advance() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let [x, y] = [bed.add(), bed.add()];
    for s in [x, y] {
        bed.write(s, 64);
        bed.drain();
    }
    assert_eq!(bed.v(), 32);
    bed.fin_read(x);
    assert_eq!((bed.advance(x), bed.v()), (0, 16));
    bed.fin_read(y);
    assert_eq!((bed.advance(y), bed.v()), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn abort_returns_the_advance() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let [x, y] = [bed.add(), bed.add()];
    for s in [x, y] {
        bed.write(s, 64);
        bed.drain();
    }
    assert_eq!(bed.v(), 32);
    let x = bed.streams.remove(x);
    x.handle.abort(bed.selector.strategy_mut());
    assert_eq!(bed.v(), 16);
    let y = bed.streams.remove(0);
    y.handle.abort(bed.selector.strategy_mut());
    assert_eq!(bed.v(), 0);
}

#[tokio::test(start_paused = true)]
async fn the_budget_is_shared_and_a_stream_waits_for_it() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let [x, y, z] = [bed.add(), bed.add(), bed.add()];
    for s in [x, y] {
        bed.write(s, 64);
        bed.drain();
    }
    assert_eq!(bed.v(), 32, "the budget is spent");

    // Z has only its base window, and waits for the budget, outside of the turn queue.
    bed.write(z, 64);
    assert_eq!(bed.drain(), [(z, 8), (z, 8)]);
    assert!(bed.waits_for_budget(z));
    assert!(!bed.is_queued(z));
    assert_eq!(bed.advance(z), 0);
}

#[tokio::test(start_paused = true)]
async fn a_repayment_wakes_the_stream_that_waits_for_the_budget() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let [x, y, z] = [bed.add(), bed.add(), bed.add()];
    for s in [x, y, z] {
        bed.write(s, 64);
        bed.drain();
    }
    assert!(bed.waits_for_budget(z));

    // 8 bytes are repaid by X. Z never gets credit of its own: it is the repayment that wakes it.
    // X sends 16 of what its credit proved.
    bed.credit(x, 8);
    assert_eq!(bed.drain_grouped(), [(x, 8), (x, 8), (z, 8)]);
    assert_eq!((bed.advance(z), bed.v()), (8, 32));
    assert!(
        bed.waits_for_budget(z),
        "it took what was free and waits again"
    );
}

#[tokio::test(start_paused = true)]
async fn the_budget_goes_to_the_waiters_in_order_and_the_wake_up_is_passed_on() {
    // One grant is 8 bytes, the budget 16.
    let mut bed = Bed::with_advance(0.125, 0.25);
    let [x, y, z, w, u] = [bed.add(), bed.add(), bed.add(), bed.add(), bed.add()];
    for s in [x, y, z, w, u] {
        bed.write(s, 64);
        bed.drain();
    }
    assert_eq!(bed.v(), 16);
    for s in [z, w, u] {
        assert!(bed.waits_for_budget(s));
    }

    // 8 bytes come back: the first waiter takes them, the others keep waiting.
    bed.fin_read(x);
    assert_eq!(bed.drain(), [(z, 8)]);
    assert!(!bed.waits_for_budget(z) && bed.waits_for_budget(w) && bed.waits_for_budget(u));
    assert_eq!(bed.v(), 16);

    // 8 bytes come back, and then 8 more before anybody polled: the woken waiter takes its part,
    // and passes the wake-up on.
    bed.fin_read(y);
    bed.fin_read(z);
    assert_eq!(bed.v(), 0);
    assert_eq!(bed.drain(), [(w, 8), (u, 8)]);
    assert_eq!(bed.v(), 16);
}

/// A deterministic sequence of writes, credits and `FIN_READ`s of six streams: `V <= G`, `advance_s <= g`,
/// and the books agree, after every step.
#[tokio::test(start_paused = true)]
async fn the_advance_stays_within_its_bounds_whatever_the_streams_do() {
    let mut bed = Bed::with_advance(0.25, 0.5);
    let streams = (0..6).map(|_| bed.add()).collect::<Vec<_>>();
    // Some readers have proven something, some have not.
    for &s in &streams[..2] {
        bed.prime(s, 24);
    }
    let mut sent = vec![0_u32; streams.len()];
    let mut closed = vec![false; streams.len()];
    let mut rng = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut max_v = 0;
    for step in 0..2000 {
        let s = streams[(next() % 6) as usize];
        match next() % 8 {
            0..=2 if !closed[s] => {
                // A write is only possible when the previous one went out.
                let ready = bed.streams[s]
                    .duplex
                    .feed(Bytes::from(vec![1; 1 + (next() % 40) as usize]))
                    .now_or_never();
                assert!(ready.is_some() || bed.unacked(s) > 0);
            },
            3..=6 if !closed[s] && sent[s] > 0 => {
                let credit = 1 + u32::try_from(next() % u64::from(sent[s])).unwrap();
                bed.credit(s, credit);
                sent[s] -= credit;
            },
            7 if step % 5 == 0 && !closed[s] => {
                bed.fin_read(s);
                closed[s] = true;
                sent[s] = 0;
            },
            _ => {},
        }
        if next() % 3 == 0 {
            tokio::time::advance(Duration::from_millis(next() % 1500)).await;
        }
        for (s, len) in bed.drain() {
            sent[s] += u32::try_from(len).unwrap();
        }

        let advances = streams.iter().map(|&s| bed.advance(s)).collect::<Vec<_>>();
        assert!(bed.v() <= 32, "step {step}: V = {}", bed.v());
        assert!(
            advances.iter().all(|&a| a <= 16),
            "step {step}: {advances:?}"
        );
        assert_eq!(advances.iter().sum::<u64>(), bed.v(), "step {step}");
        for &s in &streams {
            let borrowed = bed.unacked(s).saturating_sub(16);
            assert!(bed.advance(s) <= borrowed, "step {step}");
            assert!(bed.flow().tx_pool.used() <= 64);
        }
        max_v = max_v.max(bed.v());
    }
    assert!(max_v > 16, "the test reached more than one grant: {max_v}");
}
