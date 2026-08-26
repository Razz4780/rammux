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
    /// Used to sleep until it's time to send or timeout a `PING`.
    sleep: Pin<Box<Sleep>>,
    /// Interval on which we send `PING`s.
    interval: Duration,
    /// `PING` timeout.
    timeout: Duration,
    /// `PING` state machine.
    state: State,
}

impl OutboundPings {
    /// Creates a new instance, configured to send `PING`s on the given interval
    /// and with the given timeout.
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            timeout,
            state: State::WaitingToSend,
        }
    }

    /// If a `PING` is ready to be sent, returns it.
    ///
    /// This struct will consider the `PING` to be sent.
    pub fn take_ready(&mut self) -> Option<PingPayload> {
        match &mut self.state {
            State::WaitingToSend | State::WaitingForResponse { .. } => None,
            State::ReadyToSend { since } => {
                let payload = PingPayload::random();
                let now = Instant::now();
                self.state = State::WaitingForResponse {
                    ready_since: *since,
                    sent_at: now,
                    payload,
                };
                Some(payload)
            },
        }
    }

    /// Polls progress on this state machine.
    ///
    /// Returns when the next `PING` is ready to be sent.
    ///
    /// If this method returns an error, this struct should no longer be used.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        match &mut self.state {
            State::WaitingToSend => {
                std::task::ready!(self.sleep.poll_unpin(cx));
                let now = Instant::now();
                self.state = State::ReadyToSend { since: now };
                self.sleep.as_mut().reset(Instant::now() + self.timeout);
                let result = match self.sleep.poll_unpin(cx) {
                    Poll::Ready(()) => Err(ErrorKind::PingSendTimeout {
                        elapsed: now.elapsed(),
                    }),
                    Poll::Pending => Ok(()),
                };
                Poll::Ready(result)
            },
            State::ReadyToSend { since } => {
                std::task::ready!(self.sleep.poll_unpin(cx));
                Poll::Ready(Err(ErrorKind::PingSendTimeout {
                    elapsed: since.elapsed(),
                }))
            },
            State::WaitingForResponse {
                payload,
                ready_since,
                ..
            } => {
                std::task::ready!(self.sleep.poll_unpin(cx));
                Poll::Ready(Err(ErrorKind::PingResponseTimeout {
                    payload: *payload,
                    elapsed: ready_since.elapsed(),
                }))
            },
        }
    }

    /// Notifies this struct that we received a `PING` response.
    ///
    /// Returns the `PING` exchange latency.
    ///
    /// If this method returns an error, this struct should no longer be used.
    pub fn received_response(&mut self, payload: PingPayload) -> Result<Duration, ErrorKind> {
        match &mut self.state {
            State::WaitingToSend | State::ReadyToSend { .. } => {
                Err(ErrorKind::UnexpectedPing(payload))
            },
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
    /// Ready to send the next `PING`.
    ///
    /// [`OutboundPings::sleep`] set to elapse when the `PING` times out.
    ReadyToSend {
        /// Time when the `PING` was ready to be sent.
        ///
        /// Timeout is tracked from this point in time.
        since: Instant,
    },
    /// Waiting for a `PING` response.
    ///
    /// [`OutboundPings::sleep`] set to elapse when the `PING` times out.
    WaitingForResponse {
        /// Time when the `PING` was ready to be sent.
        ///
        /// Timeout is tracked from this point in time.
        ready_since: Instant,
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
        let mut state = OutboundPings::new(Duration::from_secs(5), Duration::from_secs(10));
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(5),
            now + Duration::from_secs(10),
        ] {
            futures::future::poll_fn(|cx| state.poll_progress(cx))
                .await
                .unwrap();
            let payload = state.take_ready().unwrap();
            assert_eq!(Instant::now(), ready_at);
            tokio::time::sleep(Duration::from_secs(2)).await;
            state.received_response(payload).unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ping_timeout_ping_not_sent() {
        let mut state = OutboundPings::new(Duration::from_secs(5), Duration::from_secs(10));
        let now = Instant::now();
        futures::future::poll_fn(|cx| state.poll_progress(cx))
            .await
            .unwrap();
        assert_eq!(Instant::now(), now);
        futures::future::poll_fn(|cx| state.poll_progress(cx))
            .await
            .unwrap_err();
        assert_eq!(Instant::now(), now + Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn ping_timeout_ping_response_not_received() {
        let mut state = OutboundPings::new(Duration::from_secs(5), Duration::from_secs(10));
        let now = Instant::now();
        futures::future::poll_fn(|cx| state.poll_progress(cx))
            .await
            .unwrap();
        assert_eq!(Instant::now(), now);
        state.take_ready().unwrap();
        futures::future::poll_fn(|cx| state.poll_progress(cx))
            .await
            .unwrap_err();
        assert_eq!(Instant::now(), now + Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn ping_exceeding_interval() {
        let mut state = OutboundPings::new(Duration::from_secs(5), Duration::from_secs(10));
        let now = Instant::now();
        for ready_at in [
            now,
            now + Duration::from_secs(6),
            now + Duration::from_secs(12),
        ] {
            futures::future::poll_fn(|cx| state.poll_progress(cx))
                .await
                .unwrap();
            let payload = state.take_ready().unwrap();
            assert_eq!(Instant::now(), ready_at);
            tokio::time::sleep(Duration::from_secs(6)).await;
            state.received_response(payload).unwrap();
        }
    }
}
