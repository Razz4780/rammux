//! The downgrade handshake that hands the transport back.

use std::{
    io,
    ops::Not,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    error::{ErrorKind, RammuxError},
    transport::Transport,
};

/// [`Future`] that handles the rammux connection downgrade.
///
/// Resolves to the IO transport originally passed to the [`RammuxConnection`](super::RammuxConnection).
/// Returned transport is clean, meaning that it has no unread rammux protocol bytes.
///
/// This future should be polled to completion in order to avoid errors on the other side.
#[must_use = "downgrade should be polled to unblock the rammux peer"]
pub struct Downgraded<IO> {
    /// Recovered from [`Active::transport`](super::state::Active::transport).
    ///
    /// Boxed because this future is a variant of
    /// [`RammuxProgress`](super::RammuxProgress), which is returned by
    /// value from every poll of the connection - the downgrade happens
    /// once, so its allocation is cheaper than carrying a transport's
    /// worth of bytes through the hot path.
    transport: Option<Box<Transport<IO>>>,
    /// Whether we've received the `TERM` frame.
    term_received: bool,
    /// Whether our own `TERM` is out and flushed.
    term_sent: bool,
    /// Whether we should automatically shut down the transport.
    with_shutdown: bool,
    /// Whether the shutdown has run, if one was asked for.
    shut_down: bool,
}

impl<IO> Downgraded<IO> {
    pub(super) fn new(transport: Transport<IO>, term_received: bool) -> Self {
        Self {
            transport: Some(Box::new(transport)),
            term_received,
            term_sent: false,
            with_shutdown: false,
            shut_down: false,
        }
    }

    /// Transforms this future to automatically shut down the transport
    /// after sending the final `TERM` frame.
    ///
    /// This might save some network roundtrips if you do not want to use the transport anymore.
    pub fn with_shutdown(mut self) -> Self {
        self.with_shutdown = true;
        self
    }

    fn transport(&mut self) -> &mut Transport<IO> {
        self.transport
            .as_mut()
            .expect("Downgraded future polled after completion")
    }
}

impl<IO> Downgraded<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Sends our own `TERM` and flushes it, optionally shutting the
    /// transport down afterwards.
    fn poll_recover_writer(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RammuxError>> {
        if self.term_sent.not() {
            std::task::ready!(self.transport().poll_close_unpin(cx))?;
            self.term_sent = true;
        }
        if self.with_shutdown && self.shut_down.not() {
            std::task::ready!(self.transport().poll_shutdown(cx))?;
            self.shut_down = true;
        }
        Poll::Ready(Ok(()))
    }

    /// Reads until the peer's `TERM`, discarding whatever is still in
    /// flight: those streams are gone with the connection.
    fn poll_recover_reader(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RammuxError>> {
        if self.term_received {
            return Poll::Ready(Ok(()));
        }
        loop {
            match std::task::ready!(self.transport().poll_next_unpin(cx)) {
                Some(Ok(..)) => {},
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => {
                    self.term_received = true;
                    return Poll::Ready(Ok(()));
                },
            }
        }
    }
}

impl<IO> Future for Downgraded<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<IO, RammuxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let read_ready = this.poll_recover_reader(cx)?.is_ready();
        let write_ready = this.poll_recover_writer(cx)?.is_ready();
        if read_ready.not() || write_ready.not() {
            return Poll::Pending;
        }
        let (io, unread) = this
            .transport
            .take()
            .expect("future polled after completion")
            .into_inner();
        // The decoder never reads past a frame, so the `TERM` we just
        // consumed leaves nothing behind. Anything here would be bytes
        // the peer sent after leaving rammux, which the handshake has no
        // way to attribute - fail rather than silently swallow them.
        if unread.is_empty().not() {
            return Poll::Ready(Err(ErrorKind::Io(io::Error::other(
                "peer sent data before the downgrade handshake completed",
            ))
            .into()));
        }
        Poll::Ready(Ok(io))
    }
}
