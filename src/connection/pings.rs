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
    /// Set to elapse when it's time to send the next `PING`.
    sleep: Pin<Box<Sleep>>,
    /// Interval on which we send `PING`s.
    interval: Duration,
    /// `PING` state machine.
    state: State,
}

enum State {
    /// We're waiting to send the next `PING` frame.
    WaitingToSend,
    /// We should send the next `PING` frame now.
    ReadyToSend,
    /// We've sent the `PING` frame and we're waiting for the response.
    Sent(PingPayload, Instant),
}

impl OutboundPings {
    /// Creates a new instance, configured to send `PING`s on the given interval.
    pub fn new(interval: Duration) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            interval,
            state: State::ReadyToSend,
        }
    }

    /// Polls for readiness to send the next `PING` frame.
    ///
    /// If this method fails, this instance becomes invalid and should no longer be used.
    ///
    /// # Returns
    ///
    /// * [`Ok`] if we should send `PING`.
    /// * [`Err`] if we did not receive the response on time.
    pub fn poll_should_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        match &mut self.state {
            State::WaitingToSend => {
                std::task::ready!(self.sleep.poll_unpin(cx));
                self.state = State::ReadyToSend;
                Poll::Ready(Ok(()))
            },
            State::ReadyToSend => Poll::Ready(Ok(())),
            State::Sent(payload, sent_at) => {
                std::task::ready!(self.sleep.poll_unpin(cx));
                Poll::Ready(Err(ErrorKind::PingTimeout {
                    payload: *payload,
                    elapsed: sent_at.elapsed(),
                }))
            },
        }
    }

    /// If we should send the next `PING` frame now, returns its payload.
    pub fn try_collect(&mut self) -> Option<PingPayload> {
        match &mut self.state {
            State::ReadyToSend => {
                let payload = PingPayload::random();
                let now = Instant::now();
                self.state = State::Sent(payload, now);
                self.sleep.as_mut().reset(now + self.interval);
                Some(payload)
            },
            State::WaitingToSend | State::Sent(..) => None,
        }
    }

    /// Notifies this struct that we received a `PING` response.
    pub fn received_response(&mut self, payload: PingPayload) -> Result<Duration, ErrorKind> {
        match &mut self.state {
            State::ReadyToSend | State::WaitingToSend => Err(ErrorKind::UnexpectedPing(payload)),
            State::Sent(expected, sent_at) => {
                if *expected != payload {
                    Err(ErrorKind::UnexpectedPing(payload))
                } else {
                    let elapsed = sent_at.elapsed();
                    self.state = State::WaitingToSend;
                    Ok(elapsed)
                }
            },
        }
    }
}
