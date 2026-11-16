//! Property test of the accounting: two ends of three streams exchange random traffic over a delayed link.

use std::{
    collections::VecDeque,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::time::Instant;

use crate::{
    StreamId,
    buffer::Data,
    config::RammuxConfig,
    flow::Srtt,
    stream::{
        ConnFlow, RammuxDuplex, RxFlow,
        handle::StreamHandle,
        test_support::{Selectors, fins},
    },
};

const POOL: u32 = 40;
const BASE: u32 = 16;
const STREAMS: usize = 3;

const CONFIG: RammuxConfig = RammuxConfig {
    frame_limit: NonZeroU16::new(8).unwrap(),
    max_inbound_streams: 8,
    max_outbound_streams: 8,
    local_recv_window: NonZeroU32::new(BASE).unwrap(),
    remote_recv_window: NonZeroU32::new(BASE).unwrap(),
    local_recv_pool: POOL,
    remote_recv_pool: POOL,
    advance_grant: 0.0,
    advance_budget: 0.0,
    ping_interval: Duration::from_secs(1),
    inbound_frame_timeout: Duration::from_secs(2),
};

/// A frame in flight.
struct InFlight {
    at: Instant,
    stream: usize,
    credit: u32,
    data: Bytes,
    fin_write: bool,
    fin_read: bool,
}

/// One end of the link: streams with both directions.
struct End {
    selectors: Selectors,
    handles: Vec<StreamHandle>,
    duplexes: Vec<RammuxDuplex>,
    ids: Vec<StreamId>,
    outgoing: VecDeque<InFlight>,
}

impl End {
    fn new(initiator: bool) -> Self {
        let srtt = Srtt::of(Duration::from_millis(100));
        let mut tx = ConnFlow::new(POOL);
        tx.srtt = srtt;
        let mut rx = RxFlow::new(POOL);
        rx.srtt = srtt;
        let mut end = Self {
            selectors: Selectors::new(rx, tx),
            handles: Vec::new(),
            duplexes: Vec::new(),
            ids: Vec::new(),
            outgoing: VecDeque::new(),
        };
        for n in 0..STREAMS {
            let id = StreamId::from_be_bytes((u16::try_from(n).unwrap() * 2).to_be_bytes());
            let (handle, credit, data, duplex) = crate::stream::new(id, initiator, &CONFIG);
            end.selectors.credit.push(credit);
            end.selectors.data.push(data);
            end.handles.push(handle);
            end.duplexes.push(duplex);
            end.ids.push(id);
        }
        end
    }

    /// Collects the frames the streams want to send, credit first, like the connection does.
    fn flush(&mut self, delay: Duration) {
        let Self {
            selectors,
            ids,
            outgoing,
            ..
        } = self;
        selectors.drain(|update, _| {
            let stream = ids.iter().position(|id| *id == update.id).unwrap();
            outgoing.push_back(InFlight {
                at: Instant::now() + delay,
                stream,
                credit: update.credit,
                data: update.data,
                fin_write: update.flags.fin_write,
                fin_read: update.flags.fin_read,
            });
        });
    }

    /// Delivers the frames the peer sent.
    fn deliver(&mut self, from: &mut VecDeque<InFlight>) {
        while from.front().is_some_and(|f| f.at <= Instant::now()) {
            let f = from.pop_front().unwrap();
            self.handles[f.stream]
                .received(
                    NonZeroU32::new(f.credit),
                    Data::copy_from_slice(&f.data),
                    fins(f.fin_write, f.fin_read),
                    self.selectors.data.strategy_mut(),
                    self.selectors.credit.strategy_mut(),
                )
                .expect("the peer follows the rules");
        }
    }

    fn unacked_tx(&self, s: usize) -> u64 {
        self.handles[s].0.lock().outbound.unacked()
    }

    fn unacked_rx(&self, s: usize) -> u64 {
        self.handles[s].0.lock().inbound.unacked()
    }
}

/// A tiny deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }
}

async fn run(seed: u64, steps: usize) -> u64 {
    let mut max_used = 0;
    let mut rng = Rng(seed);
    let mut left = End::new(true);
    let mut right = End::new(false);
    let mut written = [0_usize; STREAMS];
    let mut read = [0_usize; STREAMS];
    let mut lag = Duration::from_millis(30);

    let mut step =
        |left: &mut End, right: &mut End, rng: &mut Rng, lag: Duration, writing: bool| {
            // Writes by the application of the left end, reads by the right one.
            if writing && rng.next(10) < 5 {
                let s = usize::try_from(rng.next(STREAMS as u64)).unwrap();
                let len = usize::try_from(rng.next(30)).unwrap() + 1;
                if left.duplexes[s]
                    .feed(Bytes::from(vec![1; len]))
                    .now_or_never()
                    .is_some()
                {
                    written[s] += len;
                }
            }
            // The reader is bursty and sometimes slow.
            for _ in 0..rng.next(4) {
                let s = usize::try_from(rng.next(STREAMS as u64)).unwrap();
                if let Some(Some(data)) = right.duplexes[s].next().now_or_never() {
                    read[s] += data.unwrap().len();
                }
            }
            left.flush(lag);
            right.flush(lag);
            let mut to_right = std::mem::take(&mut left.outgoing);
            let mut to_left = std::mem::take(&mut right.outgoing);
            right.deliver(&mut to_right);
            left.deliver(&mut to_left);
            // What is not due yet goes back to the line.
            to_right.append(&mut left.outgoing);
            left.outgoing = to_right;
            to_left.append(&mut right.outgoing);
            right.outgoing = to_left;
        };

    for _ in 0..steps {
        step(&mut left, &mut right, &mut rng, lag, true);
        if rng.next(50) == 0 {
            lag = Duration::from_millis(rng.next(80) + 1);
        }
        tokio::time::advance(Duration::from_millis(rng.next(20))).await;

        // The sender never believes it has less in flight than the receiver does.
        let (mut sum_tx, mut sum_rx) = (0, 0);
        for s in 0..STREAMS {
            let (tx, rx) = (left.unacked_tx(s), right.unacked_rx(s));
            assert!(tx >= rx, "stream {s}: {tx} < {rx}");
            sum_tx += tx.saturating_sub(u64::from(BASE));
            sum_rx += rx.saturating_sub(u64::from(BASE));
        }
        let (g_tx, g_rx) = (
            left.selectors.data.strategy(),
            right.selectors.credit.strategy(),
        );
        assert_eq!(g_tx.tx_pool.used(), sum_tx);
        assert_eq!(g_rx.pool.used(), sum_rx);
        assert!(g_tx.tx_pool.used() >= g_rx.pool.used());
        assert!(g_tx.tx_pool.used() <= u64::from(POOL));
        max_used = max_used.max(g_tx.tx_pool.used());
    }

    // Stop writing and let everything drain: nothing is stranded.
    for _ in 0..2000 {
        step(&mut left, &mut right, &mut rng, lag, false);
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    for s in 0..STREAMS {
        assert_eq!(written[s], read[s], "stream {s} delivered everything");
        // What is left in flight is the credit below the threshold.
        assert!(
            left.unacked_tx(s) <= u64::from(BASE) / 2,
            "{}",
            left.unacked_tx(s)
        );
        assert_eq!(left.unacked_tx(s), right.unacked_rx(s));
    }
    assert_eq!(left.selectors.data.strategy().tx_pool.used(), 0);
    assert_eq!(right.selectors.credit.strategy().pool.used(), 0);
    assert_eq!(left.selectors.data.strategy().turns.len(), 0);
    max_used
}

#[tokio::test(start_paused = true)]
async fn sender_never_counts_less_than_receiver() {
    let mut max_used = 0;
    for seed in 1..=20_u64 {
        let seed = 0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0x1234_5678_9ABC_DEF1);
        max_used = max_used.max(run(seed, 1500).await);
    }
    // The runs do use the pool, up to its size.
    assert_eq!(max_used, u64::from(POOL));
}
