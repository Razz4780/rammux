//! CopaLike on a simulated link, in paused time. The assertions are on properties (queue bounds,
//! utilization, no starvation, convergence, a base delay that does not creep up), with margins,
//! not on exact numbers.
//!
//! Most scenarios run at a quarter of the scale (see [`quarter`]): the same delays and dynamics with a
//! quarter of the bytes to move, which makes the simulation faster.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::{Arc, Mutex},
    time::Duration,
};

use rstest::rstest;
use tokio::time::Instant;

use crate::{
    config::{RammuxConfig, TransitControl},
    connection::sim::{App, Competitor, Dir, Links, Path, Rig},
};

fn config() -> RammuxConfig {
    let mut config = RammuxConfig::new();
    config.transit_control = TransitControl::CopaLike;
    config.frame_limit = NonZeroU16::new(16 * 1024).unwrap();
    config.local_recv_window = NonZeroU32::new(64 * 1024).unwrap();
    config.remote_recv_window = NonZeroU32::new(64 * 1024).unwrap();
    // The pool is only a memory limit: it must not be what limits the throughput.
    config.local_recv_pool = 64 * 1024 * 1024;
    config.remote_recv_pool = 64 * 1024 * 1024;
    config.inbound_frame_timeout = Duration::from_secs(3600);
    config
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// How long the tests of a base delay that must not creep up run. A broken re-baseline test shows
/// its creep within a minute or two; these tests were checked to fail on such a break at this length.
const LONG_RUN: Duration = Duration::from_secs(120);

/// Samples of the queue delay of a direction of the link.
struct Queue {
    samples: Arc<Mutex<Vec<(Duration, f64)>>>,
}

/// What the queue was like in a window.
#[derive(Debug, Clone, Copy)]
struct QueueStats {
    mean: f64,
    p95: f64,
    max: f64,
    /// Share of the time the queue was below 1 ms.
    empty: f64,
}

impl Queue {
    fn watch(dir: &Arc<Dir>) -> Self {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let start = Instant::now();
        let (dir, out) = (dir.clone(), samples.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(ms(5));
            loop {
                tick.tick().await;
                let at = Instant::now() - start;
                out.lock()
                    .unwrap()
                    .push((at, dir.queue_delay().as_secs_f64() * 1e3));
            }
        });
        Self { samples }
    }

    /// Statistics of the queue in ms, between two times since the start.
    fn between(&self, from: Duration, to: Duration) -> QueueStats {
        let samples = self.samples.lock().unwrap();
        let mut window = samples
            .iter()
            .filter(|(at, _)| *at >= from && *at < to)
            .map(|(_, q)| *q)
            .collect::<Vec<_>>();
        assert!(!window.is_empty());
        window.sort_by(f64::total_cmp);
        let n = window.len();
        QueueStats {
            mean: window.iter().sum::<f64>() / n as f64,
            p95: window[(n * 95 / 100).min(n - 1)],
            max: window[n - 1],
            empty: window.iter().filter(|q| **q < 1.0).count() as f64 / n as f64,
        }
    }
}

/// Fraction of the capacity of a constant-rate direction that was used while `window` passed.
async fn utilization(dir: &Arc<Dir>, window: Duration) -> f64 {
    let before = dir.served();
    tokio::time::sleep(window).await;
    (dir.served() - before) / (dir.capacity_now() * window.as_secs_f64())
}

/// A delay that swings between `lo` and `hi` like a random walk with a time constant of a few seconds.
fn swing(lo: Duration, hi: Duration) -> impl Fn(Duration) -> Duration + Send + Sync + 'static {
    move |t| {
        let t = t.as_secs_f64();
        // Sines of incommensurable periods stand in for the random walk.
        let x = 0.5
            + 0.25 * (t / 2.3).sin()
            + 0.15 * (t / 5.1 + 1.0).sin()
            + 0.10 * (t / 0.9 + 2.0).sin();
        lo + (hi - lo).mul_f64(x.clamp(0.0, 1.0))
    }
}

/// One bulk upload on `mbit` / `delay`, the way back at 70 Mbit/s. Returns the queue of the forward
/// path in the startup (first 10 s) and after, and the utilization in the window after.
async fn one_bulk(
    mbit: f64,
    delay: Duration,
    total: Duration,
) -> (QueueStats, QueueStats, f64, Rig) {
    let mut rig = Rig::with_paths(
        Path::constant(mbit, delay),
        Path::constant(70.0, delay),
        config(),
        config(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(10)).await;
    let util = utilization(&rig.forward, total - secs(10)).await;
    (
        q.between(secs(0), secs(10)),
        q.between(secs(10), total),
        util,
        rig,
    )
}

/// Starts connections on a shared link, each with one bulk upload and with `extra[i]` ms more delay
/// each way. The i-th connection is created when the previous one has its stream, and writes
/// `starts[i]` seconds after that. Returns the rigs.
async fn connections(
    links: &Links,
    config: fn() -> RammuxConfig,
    starts: &[u64],
    extra: &[u64],
) -> Vec<Rig> {
    let mut rigs = Vec::new();
    for (i, start) in starts.iter().enumerate() {
        let mut rig = Rig::on_with_extra_delay(
            links,
            config(),
            config(),
            0,
            ms(extra.get(i).copied().unwrap_or(0)),
        );
        rig.bulk_app(App {
            starts: secs(*start),
            ..Default::default()
        })
        .await;
        rigs.push(rig);
    }
    rigs
}

/// Shares of the bytes read by the servers in a window.
async fn shares(rigs: &[Rig], window: Duration) -> Vec<f64> {
    let before = rigs.iter().map(|r| r.bytes(0)).collect::<Vec<_>>();
    tokio::time::sleep(window).await;
    let got = rigs
        .iter()
        .zip(before)
        .map(|(r, b)| (r.bytes(0) - b) as f64)
        .collect::<Vec<_>>();
    let total = got.iter().sum::<f64>();
    got.iter().map(|g| g / total).collect()
}

/// A quarter of the scale: rates, `alpha`, frames and windows are divided by 4, so that delays and
/// the dynamics are the same, and the simulation moves a quarter of the bytes.
fn quarter() -> RammuxConfig {
    let mut config = config();
    config.frame_limit = NonZeroU16::new(4 * 1024).unwrap();
    config.local_recv_window = NonZeroU32::new(16 * 1024).unwrap();
    config.remote_recv_window = NonZeroU32::new(16 * 1024).unwrap();
    config.queue_target_bytes = NonZeroU32::new(16 * 1024).unwrap();
    config.ping_bytes = NonZeroU32::new(8 * 1024).unwrap();
    config
}

/// Mbit/s of the usual paths (150, 70 and 10 Mbit/s), at quarter scale.
const Q150: f64 = 150.0 / 4.0;
const Q70: f64 = 70.0 / 4.0;
const Q10: f64 = 10.0 / 4.0;

/// A rig with the usual paths: `down` Mbit/s forward, `up` Mbit/s back, `delay` each way.
fn rig(down: f64, up: f64, delay: u64, scaled: bool) -> Rig {
    let config = || if scaled { quarter() } else { config() };
    Rig::with_paths(
        Path::constant(down, ms(delay)),
        Path::constant(up, ms(delay)),
        config(),
        config(),
        0,
    )
}

#[tokio::test(start_paused = true)]
async fn startup_overshoot_is_cut_and_the_queue_stays_short() {
    // 150 Mbit/s, 100 ms, full scale: the window doubles until it holds `alpha` of queue, overshoots
    // to about twice the bandwidth-delay product, and the stop of the growth and the excess cut bring
    // it back.
    let (startup, steady, util, _) = one_bulk(150.0, ms(50), secs(30)).await;
    assert!(startup.max < 700.0, "{startup:?}");
    assert!(steady.p95 < 80.0, "{steady:?}");
    assert!(util > 0.98, "{util}");
}

#[tokio::test(start_paused = true)]
async fn the_queue_is_far_shorter_than_without_delay_control() {
    // The same path with the pool as the only limit: the queue is what the pool holds.
    let mut config = config();
    config.transit_control = TransitControl::Off;
    config.local_recv_pool = 4 * 1024 * 1024;
    config.remote_recv_pool = 4 * 1024 * 1024;
    let mut rig = Rig::with_paths(
        Path::constant(150.0, ms(50)),
        Path::constant(70.0, ms(50)),
        config.clone(),
        config,
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(30)).await;
    let bloat = q.between(secs(10), secs(30));

    let (_, steady, _, _) = one_bulk(150.0, ms(50), secs(30)).await;
    assert!(bloat.mean > 100.0, "{bloat:?}");
    assert!(steady.mean < bloat.mean / 3.0, "{steady:?} {bloat:?}");
}

#[tokio::test(start_paused = true)]
async fn one_connection_at_10_mbit_fills_the_link() {
    let mut rig = rig(Q10, Q70, 25, true);
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(10)).await;
    let util = utilization(&rig.forward, secs(30)).await;
    // The queue averages `alpha` (52 ms here) and the swing of the loop is about as large, so the
    // queue empties some 4-7% of the time and the utilization is a little below 1.
    assert!(util > 0.92, "{util}");
    // `alpha` is 52 ms of queue here.
    let steady = q.between(secs(10), secs(40));
    assert!(steady.max < 900.0, "{steady:?}");
}

#[tokio::test(start_paused = true)]
async fn the_rebaseline_test_does_not_ratchet_at_low_bdp() {
    // At 10 Mbit/s the bandwidth-delay product is below `alpha`. If the queue never emptied, the
    // re-baseline test would take part of our own queue for the delay of the path again and again,
    // and the queue would grow. Because the queue reaches nearly empty now and then, the connection
    // holds about `alpha` (52 ms).
    let mut rig = rig(Q10, Q70, 25, true);
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(120)).await;
    let queue = q.between(secs(90), secs(120)).mean;
    assert!(queue < 100.0, "{queue}");
}

#[tokio::test(start_paused = true)]
async fn four_connections_share_equally_and_the_queue_is_theirs_together() {
    // 150 Mbit/s: each holds `alpha`, so about 4 alpha / C = 14 ms, plus what the test absorbs.
    let links = Links::new(Path::constant(Q150, ms(50)), Path::constant(Q70, ms(50)));
    let q = Queue::watch(&links.forward);
    let rigs = connections(&links, quarter, &[0, 2, 4, 6], &[]).await;
    tokio::time::sleep(secs(20)).await;
    let shares = shares(&rigs, secs(20)).await;
    let queue = q.between(secs(20), secs(40));
    for share in &shares {
        assert!((share - 0.25).abs() < 0.05, "{shares:?}");
    }
    assert!(queue.mean < 150.0, "{queue:?}");
    // Nearly never empty.
    assert!(
        queue.empty < 0.05,
        "the queue of four connections nearly never empties, as designed: {queue:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn four_connections_at_10_mbit_make_a_long_queue_but_nobody_starves() {
    // The known limit: the queue grows with the number of our connections (N alpha / C).
    let links = Links::new(Path::constant(Q10, ms(25)), Path::constant(Q70, ms(25)));
    let q = Queue::watch(&links.forward);
    let rigs = connections(&links, quarter, &[0, 2, 4, 6], &[]).await;
    tokio::time::sleep(secs(40)).await;
    let shares = shares(&rigs, secs(40)).await;
    let queue = q.between(secs(40), secs(80));
    assert!(shares.iter().all(|share| *share > 0.12), "{shares:?}");
    assert!(queue.mean < 2000.0, "{queue:?}");
}

#[tokio::test(start_paused = true)]
async fn bulk_both_ways_keeps_both_directions_busy_and_short() {
    // The client uploads at 150 and downloads at 70 at once. The reports of each side queue behind
    // the data of the other: fwd_in_flight keeps the two windows from collapsing and bursting.
    let mut rig = rig(Q150, Q70, 50, true);
    let (forward, backward) = (Queue::watch(&rig.forward), Queue::watch(&rig.backward));
    rig.stream(Some(App::default()), Some(App::default())).await;
    tokio::time::sleep(secs(15)).await;
    let (f0, b0) = (rig.forward.served(), rig.backward.served());
    tokio::time::sleep(secs(30)).await;
    let util_forward = (rig.forward.served() - f0) / (Q150 * 1e6 / 8.0 * 30.0);
    let util_backward = (rig.backward.served() - b0) / (Q70 * 1e6 / 8.0 * 30.0);
    assert!(util_forward > 0.9, "{util_forward}");
    assert!(util_backward > 0.88, "{util_backward}");
    let (qf, qb) = (
        forward.between(secs(15), secs(45)),
        backward.between(secs(15), secs(45)),
    );
    assert!(qf.p95 < 100.0, "{qf:?}");
    assert!(qb.p95 < 100.0, "{qb:?}");
}

#[tokio::test(start_paused = true)]
async fn a_capacity_drop_is_shed_within_a_few_seconds() {
    // 150 -> 10 Mbit/s at 20 s. Steps of alpha per round trip could not shed the excess while the
    // round trip itself is seconds long: the excess cut sheds it in about one round trip.
    let rate = |t: Duration| if t < secs(20) { Q150 } else { Q10 };
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)).with_rate(rate),
        Path::constant(Q70, ms(50)),
        quarter(),
        quarter(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(60)).await;
    // The queue peaks near the round trip it takes to learn about the drop...
    let peak = q.between(secs(20), secs(30));
    assert!(peak.max > 500.0, "{peak:?}");
    // ... and is below 150 ms again within about 4 s of the drop (the delay it takes to see
    // the excess is its own queue, 1.7 s).
    let recovered = (21..30).find(|t| q.between(secs(*t), secs(*t + 1)).mean < 150.0);
    assert!(recovered.is_some_and(|t| t <= 24), "{recovered:?}");
    // After that it stays bounded and the link is used.
    let after = q.between(secs(26), secs(60));
    assert!(after.max < 800.0, "{after:?}");
}

#[tokio::test(start_paused = true)]
async fn a_swinging_delay_does_not_stall_or_bloat() {
    // The delay of the path swings between 50 and 100 ms each way.
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)).with_delay(swing(ms(50), ms(100))),
        Path::constant(Q70, ms(50)).with_delay(swing(ms(50), ms(100))),
        quarter(),
        quarter(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(20)).await;
    let util = utilization(&rig.forward, secs(40)).await;
    let queue = q.between(secs(20), secs(60));
    assert!(util > 0.82, "{util}");
    assert!(queue.p95 < 100.0, "{queue:?}");
}

#[tokio::test(start_paused = true)]
async fn jitter_of_the_path_does_not_bloat_or_stall() {
    // 10 ms of jitter on every packet: the standing minimum over srtt / 2 filters it out.
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)).with_jitter(ms(10)),
        Path::constant(Q70, ms(50)).with_jitter(ms(10)),
        quarter(),
        quarter(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(10)).await;
    let util = utilization(&rig.forward, secs(40)).await;
    let queue = q.between(secs(10), secs(50));
    // How much of the jitter is left in the standing minimum depends on how many samples it sees,
    // which is a few `PING`s per round trip.
    assert!(util > 0.95, "{util}");
    assert!(queue.p95 < 100.0, "{queue:?}");
}

#[tokio::test(start_paused = true)]
async fn per_packet_jitter_is_not_taken_for_our_queue() {
    // A 25 Mbit/s path with 20 ms of jitter per packet, order kept. Under load every packet waits
    // for the slowest one before it, so a loaded path is slower than the fastest sample ever seen by
    // most of the jitter, and the minimum of a few samples in srtt / 2 never comes down to the minimum
    // of thousands: a phantom delay of the order of the jitter. The re-baseline test removes it.
    let mut rig = Rig::with_paths(
        Path::constant(25.0, ms(15)).with_jitter(ms(20)),
        Path::constant(Q70, ms(50)),
        quarter(),
        quarter(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(10)).await;
    let util = utilization(&rig.forward, secs(40)).await;
    assert!(q.between(secs(10), secs(50)).p95 < 50.0);
    assert!(util > 0.95, "{util}");
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn the_base_delay_does_not_ratchet_over_minutes(#[values(Q10, 25.0)] mbit: f64) {
    // The re-baseline test with one connection over minutes: the queue must stay where it starts
    // (about 67 ms at 10 Mbit/s, about 7.5 ms at 100 Mbit/s).
    let mut rig = rig(mbit, Q70, 25, true);
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(LONG_RUN).await;
    let (early, late) = (
        q.between(secs(20), secs(60)),
        q.between(LONG_RUN - secs(40), LONG_RUN),
    );
    assert!(late.mean < 1.15 * early.mean, "{early:?} {late:?}");
}

#[tokio::test(start_paused = true)]
async fn two_connections_do_not_ratchet_over_minutes() {
    // Two connections with the same round trip: their joint queue empties often enough for neither to
    // take the other's queue for the path.
    let links = Links::new(Path::constant(Q150, ms(50)), Path::constant(Q70, ms(50)));
    let q = Queue::watch(&links.forward);
    let _rigs = connections(&links, quarter, &[0, 5], &[]).await;
    tokio::time::sleep(LONG_RUN).await;
    let (early, late) = (
        q.between(secs(20), secs(60)),
        q.between(LONG_RUN - secs(40), LONG_RUN),
    );
    assert!(late.mean < 1.15 * early.mean, "{early:?} {late:?}");
    assert!(late.mean < 20.0, "{late:?}");
}

#[tokio::test(start_paused = true)]
async fn two_connections_with_different_round_trips_do_not_ratchet() {
    // With 100 ms more round trip on one, the two cycles do not line up and the joint queue does
    // not empty within 5 round trips of each. A re-baseline test that wanted the queue to empty
    // completely (10% of the peak) would take part of each other's queue for the path, and the
    // base delay would ratchet up for as long as the connections run. A quarter of the standing peak
    // tolerates the part that never drains.
    let links = Links::new(Path::constant(Q150, ms(50)), Path::constant(Q70, ms(50)));
    let q = Queue::watch(&links.forward);
    let _rigs = connections(&links, quarter, &[0, 5], &[0, 50]).await;
    tokio::time::sleep(LONG_RUN).await;
    let (early, late) = (
        q.between(secs(20), secs(60)),
        q.between(LONG_RUN - secs(40), LONG_RUN),
    );
    assert!(late.mean < 30.0, "{early:?} {late:?}");
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn a_drifting_clock_of_the_peer_does_not_matter(#[values(50, -50)] ppm: i64) {
    // The one-way delay mixes the clock of the peer with ours: at 50 ppm it moves by 5 ms in 100 s.
    // A clock that runs slow just lowers the samples. One that runs fast is a phantom delay, which
    // the re-baseline test removes.
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)),
        Path::constant(Q70, ms(50)),
        quarter(),
        quarter(),
        ppm,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(10)).await;
    let util = utilization(&rig.forward, secs(50)).await;
    let queue = q.between(secs(10), secs(60));
    assert!(util > 0.97, "{util}");
    // A fast peer clock re-baselines often, and the queue stays bounded.
    assert!(queue.p95 < 100.0, "{queue:?}");
    if ppm > 0 {
        assert!(
            rig.delay_stats().rebaselines_fwd > 0,
            "{:?}",
            rig.delay_stats()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_cubic_like_competitor_gets_most_of_the_link_and_we_do_not_starve() {
    // By design CopaLike does not compete with buffer-filling traffic.
    let buffer = (Q150 * 1e6 / 8.0 * 0.3) as usize;
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)).with_foreign_buffer(buffer),
        Path::constant(Q70, ms(50)),
        quarter(),
        quarter(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    let competitor = Competitor::start(&rig.forward, secs(0));
    rig.bulk(None).await;
    tokio::time::sleep(secs(20)).await;
    let (theirs0, ours0) = (competitor.bytes(), rig.bytes(0));
    tokio::time::sleep(secs(60)).await;
    let capacity = Q150 * 1e6 / 8.0 * 60.0;
    let ours = (rig.bytes(0) - ours0) as f64 / capacity;
    let theirs = (competitor.bytes() - theirs0) as f64 / capacity;
    assert!(ours > 0.07, "ours {ours} theirs {theirs}");
    assert!(theirs > 0.5, "ours {ours} theirs {theirs}");
    // The queue is the competitor's: it is as long as its buffer allows.
    assert!(q.between(secs(20), secs(80)).mean > 150.0);
}

#[tokio::test(start_paused = true)]
async fn after_an_idle_period_the_window_is_kept_and_the_link_fills_at_once() {
    let mut rig = rig(Q150, Q70, 50, true);
    rig.bulk_app(App {
        pause: Some((secs(10), secs(40))),
        ..Default::default()
    })
    .await;
    // The application pauses at 10 s. A second later the data it sent has been delivered and reported.
    tokio::time::sleep(secs(11)).await;
    let before = rig.delay_stats().window;
    assert!(
        (100_000..3_000_000).contains(&before),
        "{:?}",
        rig.delay_stats()
    );
    tokio::time::sleep(secs(28)).await;
    // There is no idle handling: the window is kept, and does not grow while the application does
    // not limit it. The pacer bounds the burst of the restart.
    assert_eq!(rig.delay_stats().window, before, "{:?}", rig.delay_stats());

    // It restarts from that window, and is at 90% of the link within a few round trips.
    // The application restarts at 40 s.
    tokio::time::sleep(secs(1)).await;
    let mut last = rig.forward.served();
    let mut reached = None;
    for i in 0..150 {
        tokio::time::sleep(ms(100)).await;
        let now = rig.forward.served();
        if (now - last) / (Q150 * 1e6 / 8.0 * 0.1) > 0.9 {
            reached = Some(f64::from(i + 1) / 10.0);
            break;
        }
        last = now;
    }
    assert!(reached.is_some_and(|t| t < 1.2), "{reached:?}");
}

#[rstest]
#[tokio::test(start_paused = true)]
async fn two_connections_converge_to_equal_shares(#[values(0, 50)] extra: u64) {
    // The second starts 5 s later; in some cases it also has 100 ms more round trip.
    let links = Links::new(Path::constant(Q150, ms(50)), Path::constant(Q70, ms(50)));
    let rigs = connections(&links, quarter, &[0, 5], &[0, extra]).await;
    tokio::time::sleep(secs(25)).await;
    let shares = shares(&rigs, secs(30)).await;
    assert!(
        shares.iter().all(|share| (0.38..=0.62).contains(share)),
        "{shares:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_window_does_not_cut_frames_into_pieces() {
    // Silly window syndrome: room in the window opens a few bytes at a time, with every report. A sender
    // that fills every opening sends tiny frames, each with a PING, and the headers and the CPU of both
    // peers become the bottleneck. Every write costs the header of a segment on the simulated link,
    // so such frames cost throughput here too.
    let mut rig = rig(Q150, Q70, 50, true);
    rig.bulk(None).await;
    tokio::time::sleep(secs(10)).await;
    let (frames0, bytes0) = rig.forward.writes();
    let (backward0, _) = rig.backward.writes();
    tokio::time::sleep(secs(30)).await;
    let (frames1, bytes1) = rig.forward.writes();
    let (backward1, _) = rig.backward.writes();
    let (frames, bytes) = (frames1 - frames0, bytes1 - bytes0);
    let mean = bytes as f64 / frames as f64;
    let frame_limit = 4096.0;
    assert!(
        mean >= frame_limit / 2.0,
        "mean frame {mean} bytes, {frames} frames in 30 s"
    );
    // The reports come back at most a few times per frame sent.
    assert!(
        (backward1 - backward0) < 2 * frames,
        "{} reports for {frames} frames",
        backward1 - backward0
    );
}

#[tokio::test(start_paused = true)]
async fn a_stall_of_the_forward_path_does_not_open_the_window() {
    // The forward path stops for two round trips: there are no reports. The data in flight stays
    // in flight; the window does not open as time passes (the bytes assumed delivered do not grow
    // with the time since the last report), and what the sender puts on the path is what the
    // window had room for when the stall began, not more.
    let stall = |t: Duration| {
        if t >= secs(20) && t < secs(20) + ms(400) {
            0.000_001
        } else {
            Q150
        }
    };
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)).with_rate(stall),
        Path::constant(Q70, ms(50)),
        quarter(),
        quarter(),
        0,
    );
    rig.bulk(None).await;
    // The reports of what had passed the link when it stopped still arrive for about a round trip.
    // After that there are none, and the sender may send only what is left of its window.
    tokio::time::sleep(secs(20) + ms(200)).await;
    let (_, before) = rig.forward.writes();
    let stats = rig.delay_stats();
    let window = stats.window;
    let room = window.saturating_sub(stats.fwd_in_flight);
    tokio::time::sleep(ms(200)).await;
    let (_, after) = rig.forward.writes();
    let sent = after - before;
    let frame = 4096;
    assert!(
        sent <= room + 2 * frame,
        "sent {sent} bytes with no reports, window {window}, room {room}"
    );
}

/// What the invariant "the queue touches nearly empty within each re-baseline history" looks like
/// from outside, at 10 Mbit/s (40 ms of round trip, a bandwidth-delay product of half of `alpha`).
/// Returns:
/// - the re-baselines the connection made after the first 20 s;
/// - the mean queue early (20-60 s) and late (the last 40 s);
/// - the smallest, over the 1 s windows from 20 s on, of the share of samples with the queue below
///   1 ms: nonzero if every such window had a moment with an empty queue.
///
/// The excess cut over-cuts a falling queue by about `1 + q / d`, and that is what empties the queue
/// here. With its gain halved the base delay creeps up.
async fn low_bdp_floor(total: u64) -> (u64, f64, f64, f64) {
    let mut rig = rig(Q10, Q70, 25, true);
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(20)).await;
    let before = rig.delay_stats().rebaselines_fwd;
    tokio::time::sleep(secs(total - 20)).await;
    let rebaselines = rig.delay_stats().rebaselines_fwd - before;
    let (early, late) = (
        q.between(secs(20), secs(60)).mean,
        q.between(secs(total - 40), secs(total)).mean,
    );
    // The deepest trough: the worst window of 1 s.
    let worst = (20..total)
        .map(|t| {
            let w = q.between(secs(t), secs(t + 1));
            w.empty
        })
        .fold(f64::MAX, f64::min);
    (rebaselines, early, late, worst)
}

#[tokio::test(start_paused = true)]
async fn the_floor_does_not_creep_at_low_bdp() {
    let (rebaselines, early, late, worst_empty) = low_bdp_floor(120).await;
    // Every window of 1 s had a moment with an empty queue.
    assert!(worst_empty > 0.0, "{worst_empty}");
    assert!(late < 1.15 * early && late < 100.0, "{early} {late}");
    assert!(rebaselines <= 2, "{rebaselines}");
}

/// The gain of 1 of the excess cut is load-bearing. The `own` that the cut reads is a round trip old,
/// so the cut overshoots the queue it meets by about `1 + q / d`, and the overshoot empties the
/// queue after a drop (and, in the steady state of a low bandwidth-delay product, is what lets the
/// re-baseline test see an empty queue). A gain of 1/2 sheds the same excess more slowly and leaves
/// the queue standing: the bound below on the queue summed over the 3 s after a drop to a fifth of
/// the capacity fails with half the gain.
#[tokio::test(start_paused = true)]
async fn the_excess_cut_overshoots_so_a_capacity_drop_is_shed_fast() {
    let rate = |t: Duration| if t < secs(20) { Q150 } else { Q150 / 5.0 };
    let mut rig = Rig::with_paths(
        Path::constant(Q150, ms(50)).with_rate(rate),
        Path::constant(Q70, ms(50)),
        quarter(),
        quarter(),
        0,
    );
    let q = Queue::watch(&rig.forward);
    rig.bulk(None).await;
    tokio::time::sleep(secs(40)).await;
    let shed: f64 = (20..23).map(|t| q.between(secs(t), secs(t + 1)).mean).sum();
    assert!(shed < 330.0, "{shed}");
    // And the queue gets nearly empty on the way: the over-cut.
    let touched = (20..24).any(|t| q.between(secs(t), secs(t + 1)).empty > 0.2);
    assert!(touched, "the queue never got nearly empty after the drop");
}
