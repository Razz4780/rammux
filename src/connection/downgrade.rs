use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    codec::{RammuxCodec, decoder::DecodedFrame, encoder::EncoderItem},
    error::{ErrorKind, RammuxError},
};

/// [`Future`] that handles the rammux connection downgrade.
///
/// Resolves to the IO transport originally passed to the [`RammuxConnection`](super::RammuxConnection).
/// Returned transport is clean, meaning that it has no unread rammux protocol bytes.
///
/// This future should be polled to completion in order to avoid errors on the other side.
#[must_use = "downgrade should be polled to unblock the rammux peer"]
pub struct Downgraded<IO> {
    /// Recovered from [`Active::codec`](super::state::Active::codec).
    codec: Option<RammuxCodec<IO>>,
    /// Whether we've received the `TERM` frame.
    term_received: bool,
    /// Describes the state of our `TERM` frame.
    term_sent: TermSendState,
    /// Whether we should automatically shut down the transport.
    with_shutdown: bool,
}

impl<IO> Downgraded<IO> {
    pub(super) fn new(codec: RammuxCodec<IO>, term_received: bool) -> Self {
        Self {
            codec: Some(codec),
            term_received,
            term_sent: TermSendState::Init,
            with_shutdown: false,
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
}

impl<IO> Downgraded<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_recover_writer(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        let codec = self
            .codec
            .as_mut()
            .expect("Downgraded future polled after completion");

        loop {
            match self.term_sent {
                TermSendState::Init => {
                    std::task::ready!(codec.poll_ready_unpin(cx))?;
                    codec.start_send_unpin(EncoderItem::new_terminate())?;
                    self.term_sent = TermSendState::Enqueued;
                },
                TermSendState::Enqueued => {
                    if self.with_shutdown {
                        std::task::ready!(codec.poll_close_unpin(cx))?;
                        self.term_sent = TermSendState::ShutDown;
                    } else {
                        std::task::ready!(codec.poll_flush_unpin(cx))?;
                        self.term_sent = TermSendState::Flushed;
                    }
                },
                TermSendState::Flushed if self.with_shutdown => {
                    std::task::ready!(codec.poll_close_unpin(cx))?;
                    self.term_sent = TermSendState::ShutDown;
                },
                TermSendState::Flushed | TermSendState::ShutDown => break Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_recover_reader(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ErrorKind>> {
        if self.term_received {
            return Poll::Ready(Ok(()));
        }

        let codec = self
            .codec
            .as_mut()
            .expect("Downgraded future polled after completion");
        loop {
            let item = std::task::ready!(codec.poll_next_unpin(cx))
                .ok_or(io::ErrorKind::UnexpectedEof)
                .map_err(io::Error::from)??;
            if let DecodedFrame::Terminate = item {
                self.term_received = true;
                break Poll::Ready(Ok(()));
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TermSendState {
    Init,
    Enqueued,
    Flushed,
    ShutDown,
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
        if read_ready && write_ready {
            Poll::Ready(Ok(this
                .codec
                .take()
                .expect("future polled after completion")
                .into_inner()))
        } else {
            Poll::Pending
        }
    }
}
