use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tokio::time::{Instant, Sleep};

use crate::{error::ErrorKind, header::PingPayload};

/// Tracks the state of `PING` exchanges initiated by us.
pub struct OutboundPings {
    /// Used to sleep until it's time to send a `PING`.
    sleep: Pin<Box<Sleep>>,
    /// Interval on which we send `PING`s.
    interval: Duration,
    /// `PING` state machine.
    state: State,
}

impl OutboundPings {
    /// Creates a new instance, configured to send `PING`s on the given interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            state: State::WaitingToSend,
        }
    }

    /// Polls until the next `PING` should be sent.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<PingPayload> {
        match &mut self.state {
            State::WaitingToSend => {
                std::task::ready!(self.sleep.poll_unpin(cx));
                let payload = PingPayload::random();
                let sent_at = Instant::now();
                self.state = State::WaitingForResponse { sent_at, payload };
                Poll::Ready(payload)
            },
            State::WaitingForResponse { .. } => Poll::Pending,
        }
    }

    /// Notifies this struct that we received a `PING` response.
    ///
    /// Returns the `PING` exchange latency.
    ///
    /// If this method returns an error, this struct should no longer be used.
    pub fn received_response(&mut self, payload: PingPayload) -> Result<Duration, ErrorKind> {
        match &mut self.state {
            State::WaitingToSend => Err(ErrorKind::UnexpectedPing(payload)),
            State::WaitingForResponse {
                sent_at,
                payload: expected,
                ..
            } => {
                if *expected != payload {
                    Err(ErrorKind::UnexpectedPing(payload))
                } else {
                    let elapsed = sent_at.elapsed();
                    self.sleep.as_mut().reset(*sent_at + self.interval);
                    self.state = State::WaitingToSend;
                    Ok(elapsed)
                }
            },
        }
    }
}

/// Outbound `PINGS` state machine.
enum State {
    /// Waiting to send the next `PING`.
    ///
    /// [`OutboundPings::sleep`] set to elapse when the time comes.
    WaitingToSend,
    /// Waiting for a `PING` response.
    ///
    /// [`OutboundPings::sleep`] set to elapse when the `PING` times out.
    WaitingForResponse {
        /// Time when the `PING` was sent.
        ///
        /// Used to measure latency.
        sent_at: Instant,
        /// Payload of the sent `PING`.
        ///
        /// Used to verify the response.
        payload: PingPayload,
    },
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::connection::pings::OutboundPings;

    #[tokio::test(start_paused = true)]
    async fn pings_within_interval() {
        let mut state = OutboundPings::new(Duration::from_secs(5));
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(5),
            now + Duration::from_secs(10),
        ] {
            let payload = futures::future::poll_fn(|cx| state.poll_next(cx)).await;
            assert_eq!(Instant::now(), ready_at);
            tokio::time::sleep(Duration::from_secs(2)).await;
            state.received_response(payload).unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ping_exceeding_interval() {
        let mut state = OutboundPings::new(Duration::from_secs(5));
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(6),
            now + Duration::from_secs(12),
        ] {
            let payload = futures::future::poll_fn(|cx| state.poll_next(cx)).await;
            assert_eq!(Instant::now(), ready_at);
            tokio::time::sleep(Duration::from_secs(6)).await;
            state.received_response(payload).unwrap();
        }
    }
}
