//! The steps in which a receiving direction returns credit, with the connection's timer that settles them.
//!
//! The base window is 64 KiB, so `T_c` is 32 KiB. The direction is polled the way the connection polls it:
//! only after its task was woken, by a read or by the timer.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::task::{ArcWake, AtomicWaker};
use tokio::time::Instant;

use crate::{
    buffer::Data,
    flow::Srtt,
    stream::{RxFlow, inbound::InboundTraffic},
};

const KIB: usize = 1024;
const BASE: u32 = 64 * 1024;
/// `T_c` of [`BASE`].
const T_C: u32 = 32 * 1024;

struct Flag(AtomicBool);

impl ArcWake for Flag {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::SeqCst);
    }
}

/// A receiving direction, and the connection that polls its credit task.
struct Rx {
    inbound: InboundTraffic,
    flow: RxFlow,
    credit_task: AtomicWaker,
    /// Set when the credit task is woken.
    woken: Arc<Flag>,
    task: Waker,
    /// The credit sent, and when.
    credits: Vec<(Instant, u32)>,
    consumed: u64,
}

impl Rx {
    fn new(srtt: Option<Duration>) -> Self {
        let mut flow = RxFlow::new(16 * 1024 * 1024);
        if let Some(srtt) = srtt {
            flow.srtt = Srtt::of(srtt);
        }
        // A new task is polled right away.
        let woken = Arc::new(Flag(AtomicBool::new(true)));
        Self {
            inbound: InboundTraffic::new(BASE),
            flow,
            credit_task: AtomicWaker::new(),
            task: futures::task::waker(woken.clone()),
            woken,
            credits: Vec::new(),
            consumed: 0,
        }
    }

    /// `len` bytes arrive, and the application reads them at once.
    fn consume(&mut self, len: usize) {
        self.inbound
            .received_data(Data::copy_from_slice(&vec![0; len]), &mut self.flow.pool)
            .unwrap();
        let Poll::Ready(Some(Ok(data))) = self.inbound.poll_read(Waker::noop(), &self.credit_task)
        else {
            panic!("the data is ready");
        };
        assert_eq!(data.as_ref().len(), len);
        self.consumed += len as u64;
    }

    /// Polls like the connection: the timer, the credit task if it was woken, and the timer again.
    /// Returns the credit sent.
    fn poll(&mut self) -> Vec<u32> {
        let mut cx = Context::from_waker(Waker::noop());
        self.flow.settle.poll(&mut cx);
        let mut sent = Vec::new();
        if self.woken.0.swap(false, Ordering::SeqCst) {
            loop {
                match self
                    .inbound
                    .poll_update(&mut self.flow, Instant::now(), &self.task)
                {
                    Poll::Ready((credit, fin_read)) => {
                        assert!(!fin_read);
                        sent.push(credit);
                        self.credits.push((Instant::now(), credit));
                    },
                    Poll::Pending => {
                        self.credit_task.register(&self.task);
                        break;
                    },
                }
            }
        }
        self.flow.settle.poll(&mut cx);
        sent
    }

    fn credited(&self) -> u64 {
        self.credits
            .iter()
            .map(|(_, credit)| u64::from(*credit))
            .sum()
    }

    /// Reads 8 KiB every millisecond for a second, 640 KiB per round trip of 80 ms.
    async fn read_fast_for_a_second() -> Self {
        let mut rx = Self::new(Some(Duration::from_millis(80)));
        for _ in 0..1000 {
            tokio::time::advance(Duration::from_millis(1)).await;
            rx.consume(8 * KIB);
            rx.poll();
        }
        rx
    }

    /// Reads on until the stream holds back at least `T_c`, which only a larger step does, and stops for half
    /// a round trip. Returns how long after the stop less than `T_c` was left uncredited, if it was.
    async fn stop(&mut self) -> Option<Duration> {
        while self.inbound.pending_credit() < u64::from(T_C) {
            tokio::time::advance(Duration::from_millis(1)).await;
            self.consume(8 * KIB);
            self.poll();
        }
        let stop = Instant::now();
        let mut credited = None;
        for _ in 0..40 {
            tokio::time::advance(Duration::from_millis(1)).await;
            self.poll();
            if credited.is_none() && self.inbound.pending_credit() < u64::from(T_C) {
                credited = Some(stop.elapsed());
            }
        }
        credited
    }
}

#[tokio::test(start_paused = true)]
async fn without_a_sample_credit_goes_in_the_smallest_step() {
    let mut rx = Rx::new(None);
    for _ in 0..256 {
        rx.consume(4 * KIB);
        rx.poll();
        tokio::time::advance(Duration::from_millis(1)).await;
    }
    let credits = rx
        .credits
        .iter()
        .map(|(_, credit)| *credit)
        .collect::<Vec<_>>();
    assert_eq!(credits, vec![T_C; 32]);
    assert!(rx.flow.settle.is_unused());
}

#[tokio::test(start_paused = true)]
async fn a_slow_stream_keeps_the_smallest_step() {
    let mut rx = Rx::new(Some(Duration::from_millis(100)));
    rx.poll();
    for _ in 0..20 {
        tokio::time::advance(Duration::from_millis(100)).await;
        rx.consume(32 * KIB);
        // The read that completes the step wakes the credit task.
        assert!(rx.woken.0.load(Ordering::SeqCst));
        assert_eq!(rx.poll(), [T_C]);
    }
    assert!(rx.flow.settle.is_unused());
}

#[tokio::test(start_paused = true)]
async fn the_step_follows_the_rate() {
    let rx = Rx::read_fast_for_a_second().await;
    let since = Instant::now() - Duration::from_millis(400);
    let late = rx
        .credits
        .iter()
        .filter(|(at, _)| *at > since)
        .map(|(_, credit)| *credit)
        .collect::<Vec<_>>();
    // 8 per round trip are 40.
    assert!((30..=50).contains(&late.len()), "{late:?}");
    assert!(rx.credits.iter().all(|(_, credit)| *credit >= T_C));
}

#[tokio::test(start_paused = true)]
async fn the_tail_of_a_burst_is_credited() {
    let mut rx = Rx::read_fast_for_a_second().await;
    let credited = rx.stop().await;
    // The timer looks at the stream every 10 ms, and the reader left its pace after 20 ms.
    assert!(credited.is_some());
    assert!(rx.inbound.pending_credit() < u64::from(T_C));
    assert_eq!(rx.credited() + rx.inbound.pending_credit(), rx.consumed);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_left_its_pace_starts_small_again() {
    let mut rx = Rx::read_fast_for_a_second().await;
    rx.stop().await;
    let left = rx.inbound.pending_credit();
    rx.consume(32 * KIB);
    // Due at once, and in one credit with what was left below `T_c`.
    assert_eq!(rx.poll(), [T_C + u32::try_from(left).unwrap()]);
    assert_eq!(rx.credited(), rx.consumed);
}

#[test]
fn a_tiny_round_trip_does_not_spin_the_timer() {
    let mut flow = RxFlow::new(0);
    flow.srtt = Srtt::of(Duration::from_micros(8));
    assert_eq!(flow.settle_period(), Some(Duration::from_millis(1)));
    assert_eq!(flow.settle_after(), Some(Duration::from_micros(2)));
    assert_eq!(RxFlow::new(0).settle_period(), None);
    assert_eq!(RxFlow::new(0).settle_after(), None);
}
