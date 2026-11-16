//! The `PING`/`PONG` exchange, see `PROTOCOL.md`.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::{Instant, Sleep};

use crate::wire::header::Pong;

/// The shortest wait between `PING`s while there is no round trip sample: the wait after the first `PING`.
/// Every later one doubles it.
pub const PING_BACKOFF_START: Duration = Duration::from_millis(10);

/// The shortest wait between `PING`s that ride on data, however small the round trip time is.
pub const PING_GAP_MIN: Duration = Duration::from_millis(1);

/// Our side of the `PING`/`PONG` exchange: the `PING`s we send and the `PONG`s we owe.
///
/// `PING`s and `PONG`s have no identity, and nothing waits for them.
pub struct Pings {
    schedule: PingSchedule,
    /// The answer to the latest `PING` we read, if it is not written yet. The next outgoing frame takes it.
    ///
    /// Only the latest `PING` is answered; a newer one overwrites an unanswered one. Why:
    /// 1. The memory is O(1), with no cap and no read stall: a flood of `PING`s costs only CPU, like
    ///    any flood of frames.
    /// 2. An answer that waits in a queue carries our queueing in the peer's back-delay sample.
    ///    The latest `PING` is the freshest sample.
    /// 3. Nothing on the sender's side needs one `PONG` per `PING`: every `PONG` is an independent sample.
    /// 4. No deadlock: a cap on owed `PONG`s could stop both sides from reading at the same time.
    owed_pong: Option<Pong>,
}

impl Pings {
    /// Creates the exchange of a new connection. The first `PING` is due immediately.
    pub fn new(interval: Duration) -> Self {
        Self {
            schedule: PingSchedule::new(interval),
            owed_pong: None,
        }
    }

    /// The peer sent a `PING`.
    pub fn received_ping(&mut self, t_ping: u32) {
        self.owed_pong = Some(Pong { t_echo: t_ping });
    }

    /// Returns what the next outgoing frame carries of the exchange: the `PONG` we owe,
    /// and whether it carries a `PING`.
    ///
    /// `carries_flow` says whether the frame carries `DATA` or `WINDOW`, and `srtt` is the smoothed
    /// round trip time, if there is a sample. Taking a `PING` records that it is sent at `now`.
    pub fn take_frame_part(
        &mut self,
        cx: &mut Context<'_>,
        now: Instant,
        srtt: Option<Duration>,
        carries_flow: bool,
    ) -> (Option<Pong>, bool) {
        let pong = self.owed_pong.take();
        // The interval is polled even when the piggyback rule decides, so that the wake-up is always armed.
        let on_interval = self.schedule.poll_due(cx).is_ready();
        let ping = on_interval || (carries_flow && self.schedule.piggyback_due(now, srtt));
        if ping {
            self.schedule.sent(now);
        }
        (pong, ping)
    }

    /// Returns the `PONG` we owe.
    #[cfg(test)]
    pub fn owed_pong(&self) -> Option<Pong> {
        self.owed_pong
    }
}

/// Decides when to send `PING`s.
///
/// A `PING` is due every `interval`. A frame that carries `DATA` or `WINDOW` may also carry one, if
/// enough time has passed since the last `PING`. The state of the exchange is not tracked, see `PROTOCOL.md`.
pub struct PingSchedule {
    /// Elapses when the next `PING` is due, no matter what is written.
    sleep: Pin<Box<Sleep>>,
    /// Time between `PING`s.
    interval: Duration,
    /// When the last `PING` was sent, if any.
    last: Option<Instant>,
    /// How many `PING`s were sent. Saturates.
    sent: u32,
}

impl PingSchedule {
    /// Creates a new schedule. The first `PING` is due immediately.
    pub fn new(interval: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            last: None,
            sent: 0,
        }
    }

    /// Polls until a `PING` should be sent.
    ///
    /// When this returns [`Poll::Ready`], the caller must send the `PING` and call [`Self::sent`].
    pub fn poll_due(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.sleep.poll_unpin(cx)
    }

    /// Returns whether a frame that carries `DATA` or `WINDOW` may carry a `PING` at `now`.
    ///
    /// The wait since the last `PING` is an eighth of the round trip time, so the `PING`s cost a bounded
    /// share of the frames, and are still dense enough to follow the round trip time. With no sample
    /// yet, the wait doubles with every `PING`, starting at [`PING_BACKOFF_START`].
    pub fn piggyback_due(&self, now: Instant, srtt: Option<Duration>) -> bool {
        let Some(last) = self.last else {
            return true;
        };
        let gap = match srtt {
            Some(srtt) => (srtt / 8).max(PING_GAP_MIN),
            None => {
                let doublings = self.sent.saturating_sub(1).min(31);
                PING_BACKOFF_START.saturating_mul(1 << doublings)
            },
        };
        now.saturating_duration_since(last) >= gap
    }

    /// Records that a `PING` is being sent at `now`. It restarts the interval and the gap.
    pub fn sent(&mut self, now: Instant) {
        self.last = Some(now);
        self.sent = self.sent.saturating_add(1);
        self.sleep.as_mut().reset(crate::deadline(self.interval));
    }
}

#[cfg(test)]
mod test {
    use std::{
        task::{Context, Waker},
        time::Duration,
    };

    use futures::future::poll_fn;
    use tokio::time::Instant;

    use crate::connection::pings::{PingSchedule, Pings};

    /// Asks for the `PING` of a frame at the current time.
    fn ping(pings: &mut Pings, srtt: Option<Duration>, carries_flow: bool) -> bool {
        pings
            .take_frame_part(
                &mut Context::from_waker(Waker::noop()),
                Instant::now(),
                srtt,
                carries_flow,
            )
            .1
    }

    /// Sends a `PING` on a flow frame, which must be allowed.
    fn first_ping(pings: &mut Pings) {
        assert!(ping(pings, None, true));
    }

    #[tokio::test(start_paused = true)]
    async fn pings_on_interval() {
        let mut schedule = PingSchedule::new(Duration::from_secs(5));
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(5),
            now + Duration::from_secs(10),
        ] {
            poll_fn(|cx| schedule.poll_due(cx)).await;
            assert_eq!(Instant::now(), ready_at);
            schedule.sent(Instant::now());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_flow_frame_carries_a_ping() {
        let mut pings = Pings::new(Duration::from_secs(5));
        assert!(ping(&mut pings, None, true));
    }

    #[tokio::test(start_paused = true)]
    async fn frames_without_flow_do_not_piggyback() {
        let mut pings = Pings::new(Duration::from_secs(2 * 3600));
        first_ping(&mut pings);
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert!(!ping(&mut pings, None, false));
        assert!(ping(&mut pings, None, true));
    }

    #[tokio::test(start_paused = true)]
    async fn the_gap_backs_off_before_the_first_sample() {
        let mut pings = Pings::new(Duration::from_secs(3600));
        first_ping(&mut pings);
        for gap_ms in [10, 20, 40] {
            tokio::time::advance(Duration::from_millis(gap_ms - 1)).await;
            assert!(!ping(&mut pings, None, true), "{gap_ms} ms");
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(ping(&mut pings, None, true), "{gap_ms} ms");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_backoff_saturates() {
        let interval = Duration::from_secs(5);
        let mut pings = Pings::new(interval);
        for _ in 0..200 {
            assert!(ping(&mut pings, None, false));
            tokio::time::advance(interval).await;
        }
        assert!(ping(&mut pings, None, false), "the interval still fires");
        // The gap after 200 `PING`s is evaluated here, and it is long.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!ping(&mut pings, None, true));
    }

    #[tokio::test(start_paused = true)]
    async fn a_sample_replaces_the_backoff() {
        let mut pings = Pings::new(Duration::from_secs(3600));
        first_ping(&mut pings);
        for gap_ms in [10, 20] {
            tokio::time::advance(Duration::from_millis(gap_ms)).await;
            assert!(ping(&mut pings, None, true));
        }
        let srtt = Some(Duration::from_millis(80));
        tokio::time::advance(Duration::from_millis(9)).await;
        assert!(!ping(&mut pings, srtt, true));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(ping(&mut pings, srtt, true));
    }

    #[tokio::test(start_paused = true)]
    async fn the_gap_has_a_floor() {
        let mut pings = Pings::new(Duration::from_secs(3600));
        first_ping(&mut pings);
        let srtt = Some(Duration::from_micros(80));
        tokio::time::advance(Duration::from_micros(999)).await;
        assert!(!ping(&mut pings, srtt, true));
        tokio::time::advance(Duration::from_micros(1)).await;
        assert!(ping(&mut pings, srtt, true));
    }

    #[tokio::test(start_paused = true)]
    async fn a_piggyback_ping_postpones_the_interval() {
        let mut pings = Pings::new(Duration::from_secs(5));
        let start = Instant::now();
        first_ping(&mut pings);
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(ping(&mut pings, None, true));
        tokio::time::advance(Duration::from_millis(4999)).await;
        assert!(!ping(&mut pings, None, false), "+7.999 s");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(ping(&mut pings, None, false), "+8 s");
        assert_eq!(Instant::now(), start + Duration::from_secs(8));
    }
}
