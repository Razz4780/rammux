use std::{
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Context as _;
use tokio::signal::unix::{Signal, SignalKind, signal};

/// [`Future`] that resolves when the current process receives SIGTERM or SIGINT.
pub struct ExitSignal {
    sigint: Signal,
    sigterm: Signal,
}

impl ExitSignal {
    pub fn new() -> anyhow::Result<Self> {
        let sigint =
            signal(SignalKind::interrupt()).context("failed to create a SIGINT handler")?;
        let sigterm =
            signal(SignalKind::terminate()).context("failed to create a SIGTERM handler")?;
        Ok(Self { sigint, sigterm })
    }
}

impl Future for ExitSignal {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.sigint.poll_recv(cx).is_ready() || this.sigterm.poll_recv(cx).is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
