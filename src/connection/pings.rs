use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::{Instant, Sleep};

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
    /// Value of the transmitted bytes counter when the last `PING` was sent.
    tx_bytes_at_ping: u32,
}

impl PingSchedule {
    /// Creates a new schedule. The first `PING` is due immediately.
    pub fn new(interval: Duration, ping_bytes: u32) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            ping_bytes,
            tx_bytes_at_ping: 0,
        }
    }

    /// Polls until a `PING` should be sent.
    ///
    /// `tx_bytes` is the counter of `DATA` bytes written so far.
    /// `ping_bytes_cap` lowers the number of bytes that trigger a `PING` (the window of
    /// `DelayControl` needs samples more often than that when it is small).
    /// `blocked` tells that the window keeps a frame from being sent: a `PING` is then due as
    /// soon as any data was sent since the last one, as its answer may free the window.
    /// When this returns [`Poll::Ready`], the caller must send the `PING` and call [`Self::sent`].
    pub fn poll_due(
        &mut self,
        cx: &mut Context<'_>,
        tx_bytes: u32,
        ping_bytes_cap: u32,
        blocked: bool,
    ) -> Poll<()> {
        let since = tx_bytes.wrapping_sub(self.tx_bytes_at_ping);
        if since >= self.ping_bytes.min(ping_bytes_cap.max(1)) || (blocked && since > 0) {
            return Poll::Ready(());
        }
        self.sleep.poll_unpin(cx)
    }

    /// Notifies this struct that a `PING` is being sent.
    pub fn sent(&mut self, tx_bytes: u32) {
        self.tx_bytes_at_ping = tx_bytes;
        self.sleep.as_mut().reset(Instant::now() + self.interval);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use futures::future::poll_fn;
    use tokio::time::Instant;

    use crate::connection::pings::PingSchedule;

    #[tokio::test(start_paused = true)]
    async fn pings_on_interval() {
        let mut schedule = PingSchedule::new(Duration::from_secs(5), 1000);
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(5),
            now + Duration::from_secs(10),
        ] {
            poll_fn(|cx| schedule.poll_due(cx, 0, u32::MAX, false)).await;
            assert_eq!(Instant::now(), ready_at);
            schedule.sent(0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pings_on_bytes() {
        let mut schedule = PingSchedule::new(Duration::from_secs(5), 1000);
        poll_fn(|cx| schedule.poll_due(cx, 0, u32::MAX, false)).await;
        schedule.sent(0);

        let now = Instant::now();
        assert!(
            !poll_fn(|cx| std::task::Poll::Ready(
                schedule.poll_due(cx, 999, u32::MAX, false).is_ready()
            ))
            .await
        );
        poll_fn(|cx| schedule.poll_due(cx, 1000, u32::MAX, false)).await;
        assert_eq!(Instant::now(), now);
        schedule.sent(1000);

        // The counter wraps.
        schedule.sent(u32::MAX - 10);
        assert!(
            !poll_fn(|cx| std::task::Poll::Ready(
                schedule.poll_due(cx, 500, u32::MAX, false).is_ready()
            ))
            .await
        );
        poll_fn(|cx| schedule.poll_due(cx, 989, u32::MAX, false)).await;
        assert_eq!(Instant::now(), now);

        // Sending a ping postpones the next one on the clock.
        schedule.sent(989);
        poll_fn(|cx| schedule.poll_due(cx, 989, u32::MAX, false)).await;
        assert_eq!(Instant::now(), now + Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn pings_on_a_smaller_window_and_when_blocked() {
        let ready = |schedule: &mut PingSchedule, tx, cap, blocked| {
            futures::FutureExt::now_or_never(poll_fn(|cx| schedule.poll_due(cx, tx, cap, blocked)))
                .is_some()
        };
        let mut schedule = PingSchedule::new(Duration::from_secs(5), 1000);
        poll_fn(|cx| schedule.poll_due(cx, 0, u32::MAX, false)).await;
        schedule.sent(0);

        // A cap lowers the bytes that make a PING due.
        assert!(!ready(&mut schedule, 300, u32::MAX, false));
        assert!(ready(&mut schedule, 300, 250, false));
        assert!(!ready(&mut schedule, 200, 250, false));
        // Blocked: due as soon as some data was sent since the last PING, and not before.
        assert!(!ready(&mut schedule, 0, u32::MAX, true));
        assert!(ready(&mut schedule, 1, u32::MAX, true));
        assert!(!ready(&mut schedule, 1, u32::MAX, false));
    }
}
