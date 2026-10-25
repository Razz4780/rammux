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
    global_pool::GlobalPool,
    stream::{RammuxDuplex, StreamRef, handle::StreamHandle, updates::StreamUpdates},
};

const CONFIG: RammuxConfig = RammuxConfig {
    frame_limit: NonZeroU16::new(8).unwrap(),
    max_inbound_streams: 8,
    max_outbound_streams: 8,
    local_recv_window: NonZeroU32::new(16).unwrap(),
    remote_recv_window: NonZeroU32::new(16).unwrap(),
    local_recv_pool: 1 << 20,
    remote_recv_pool: 8,
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
    selector: Selector<StreamUpdates, GlobalPool>,
    streams: Vec<Strm>,
}

impl Bed {
    /// Creates a bed where the peer's pool is `tx_pool` bytes.
    fn new(tx_pool: u32) -> Self {
        let mut global = GlobalPool::new(1 << 20, tx_pool);
        global.srtt = Some(Duration::from_secs(1));
        Self {
            selector: Selector::new(global),
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

    fn global(&self) -> &GlobalPool {
        self.selector.strategy()
    }

    /// Gives the stream an allowance of `bytes`, as if the peer's reader had just read that much.
    fn prime(&self, s: usize, bytes: u32) {
        let mut state = self.streams[s].me.lock().unwrap();
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
                false,
                false,
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
        self.global()
            .turn_order()
            .iter()
            .map(|queued| {
                self.streams
                    .iter()
                    .position(|s| Arc::ptr_eq(&s.me, queued))
                    .expect("a queued stream is a stream of the bed")
            })
            .collect()
    }

    fn is_queued(&self, s: usize) -> bool {
        self.streams[s].me.lock().unwrap().outbound.is_queued()
    }
}

/// The guard of the rule "free room never lets a stream skip the queue": it fails (and it is the only test
/// that does) if `GlobalPool::has_turn` is forced to true. The equal-share sim tests do not tell,
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
    assert_eq!(bed.global().tx_used, 8);
    assert_eq!(bed.queue(), [x]);
    // Y and Z use their base windows and queue up behind X.
    bed.write(y, 32);
    bed.write(z, 32);
    assert_eq!(bed.drain_grouped(), [(y, 8), (y, 8), (z, 8), (z, 8)]);
    assert_eq!(bed.queue(), [x, y, z]);

    // Y gets a little credit, which is not enough for a frame, and then X returns what it borrowed.
    bed.credit(y, 3);
    bed.credit(x, 8);
    assert_eq!(bed.global().tx_used, 0, "the pool is free");
    // Y is woken first, but it is not its turn: it only uses its base window.
    // Then X takes the room.
    assert_eq!(bed.drain(), [(y, 3), (x, 8)]);
    assert_eq!(bed.global().tx_used, 8);
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
        let unacked = bed.streams[slow].me.lock().unwrap().outbound.unacked();
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
    assert_eq!(bed.global().tx_used, 5);
    // And then waits for its own credit, outside of the queue.
    assert!(!bed.is_queued(x));

    // The pool has 5 bytes of room for the 8 that Y wants: the frame is 5 bytes.
    let mut bed = Bed::new(5);
    let y = bed.add();
    bed.prime(y, 1000);
    bed.write(y, 32);
    assert_eq!(bed.drain(), [(y, 8), (y, 8), (y, 5)]);
    assert_eq!(bed.global().tx_used, 5);
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
    assert_eq!(bed.global().tx_used, 2);
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
            false,
            true,
            bed.selector.strategy_mut(),
        )
        .unwrap();
    bed.drain();
    assert_eq!(bed.queue(), [z]);
    assert!(!bed.is_queued(y));

    // Z gets the room X frees.
    bed.credit(x, 8);
    assert_eq!(bed.drain(), [(z, 8)]);
    assert_eq!(bed.global().tx_used, 8);
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
    assert_eq!(bed.global().tx_used, 8);

    // The reader of X is gone: the pool is free without any credit.
    bed.streams[x]
        .handle
        .received(
            None,
            Data::default(),
            false,
            true,
            bed.selector.strategy_mut(),
        )
        .unwrap();
    assert_eq!(bed.global().tx_used, 0);
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
            false,
            false,
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
    assert_eq!(bed.global().tx_used, 24);
    assert_eq!(bed.global().turns_len(), 6);
    for round in 0..50 {
        let s = streams[round % streams.len()];
        let unacked = bed.streams[s].me.lock().unwrap().outbound.unacked();
        if unacked > 0 {
            bed.credit(s, u32::try_from(unacked.min(8)).unwrap());
        }
        bed.drain();
        assert!(bed.global().tx_used <= 24);
        let borrowed = streams
            .iter()
            .map(|&s| {
                bed.streams[s]
                    .me
                    .lock()
                    .unwrap()
                    .outbound
                    .unacked()
                    .saturating_sub(16)
            })
            .sum::<u64>();
        assert_eq!(borrowed, bed.global().tx_used);
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
            true,
            true,
            bed.selector.strategy_mut(),
        )
        .unwrap();
    bed.drain();
    assert_eq!(bed.global().turns_len(), 0);

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
    let mut global = GlobalPool::new(1 << 20, 0);
    global.srtt = Some(Duration::from_secs(1));
    let mut selector = Selector::new(global);
    let (mut handle, updates, mut duplex) =
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
            false,
            false,
            selector.strategy_mut(),
        )
        .unwrap();
    let (update, _) = selector.next().await.unwrap();
    assert_eq!(update.data.len(), 5);
}
