//! The `PING`/`PONG` exchange, see `PROTOCOL.md`.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::Sleep;

use crate::wire::header::Pong;

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
    pub fn new(interval: Duration, ping_bytes: u32) -> Self {
        Self {
            schedule: PingSchedule::new(interval, ping_bytes),
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
    /// Taking a `PING` records that it is sent.
    pub fn take_frame_part(&mut self, cx: &mut Context<'_>) -> (Option<Pong>, bool) {
        let pong = self.owed_pong.take();
        let ping = self.schedule.poll_due(cx).is_ready();
        if ping {
            self.schedule.sent();
        }
        (pong, ping)
    }

    /// Records that `DATA` is being sent.
    pub fn data_sent(&mut self, len: usize) {
        self.schedule.data_sent(len);
    }

    /// Returns the `PONG` we owe.
    #[cfg(test)]
    pub fn owed_pong(&self) -> Option<Pong> {
        self.owed_pong
    }
}

/// Decides when to send `PING`s.
///
/// A `PING` is due after every `ping_bytes` of `DATA` written, and after every `interval`.
/// The state of the exchange is not tracked, see `PROTOCOL.md`.
pub struct PingSchedule {
    /// Elapses when the next `PING` is due, no matter how much data is written.
    sleep: Pin<Box<Sleep>>,
    /// Time between `PING`s.
    interval: Duration,
    /// How many bytes of data trigger a `PING`.
    ping_bytes: u32,
    /// Bytes of `DATA` written since the last `PING`.
    unpinged_bytes: u32,
}

impl PingSchedule {
    /// Creates a new schedule. The first `PING` is due immediately.
    pub fn new(interval: Duration, ping_bytes: u32) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            ping_bytes,
            unpinged_bytes: 0,
        }
    }

    /// Polls until a `PING` should be sent.
    ///
    /// When this returns [`Poll::Ready`], the caller must send the `PING` and call [`Self::sent`].
    pub fn poll_due(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.unpinged_bytes >= self.ping_bytes {
            return Poll::Ready(());
        }
        self.sleep.poll_unpin(cx)
    }

    /// Records that a `PING` is being sent.
    pub fn sent(&mut self) {
        self.unpinged_bytes = 0;
        self.sleep.as_mut().reset(crate::deadline(self.interval));
    }

    /// Records that `DATA` is being sent.
    pub fn data_sent(&mut self, len: usize) {
        let len = u32::try_from(len).unwrap_or(u32::MAX);
        self.unpinged_bytes = self.unpinged_bytes.saturating_add(len);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use std::task::{Context, Waker};

    use futures::future::poll_fn;
    use tokio::time::Instant;

    use crate::connection::pings::PingSchedule;

    fn is_due(schedule: &mut PingSchedule) -> bool {
        schedule
            .poll_due(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    #[tokio::test(start_paused = true)]
    async fn pings_on_interval() {
        let mut schedule = PingSchedule::new(Duration::from_secs(5), 1000);
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(5),
            now + Duration::from_secs(10),
        ] {
            poll_fn(|cx| schedule.poll_due(cx)).await;
            assert_eq!(Instant::now(), ready_at);
            schedule.sent();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pings_on_bytes() {
        let mut schedule = PingSchedule::new(Duration::from_secs(5), 1000);
        poll_fn(|cx| schedule.poll_due(cx)).await;
        schedule.sent();

        let now = Instant::now();
        schedule.data_sent(999);
        assert!(!is_due(&mut schedule));
        schedule.data_sent(1);
        poll_fn(|cx| schedule.poll_due(cx)).await;
        assert_eq!(Instant::now(), now);
        schedule.sent();
        assert!(!is_due(&mut schedule), "the bytes start over");

        // Sending a ping postpones the next one on the clock.
        poll_fn(|cx| schedule.poll_due(cx)).await;
        assert_eq!(Instant::now(), now + Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn the_byte_count_saturates() {
        let mut schedule = PingSchedule::new(Duration::from_secs(5), u32::MAX);
        poll_fn(|cx| schedule.poll_due(cx)).await;
        schedule.sent();
        schedule.data_sent(usize::MAX);
        schedule.data_sent(1);
        assert!(is_due(&mut schedule));
    }
}
