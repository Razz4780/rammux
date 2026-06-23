use std::{
    fmt, io,
    num::NonZeroU32,
    ops::Not,
    task::{Poll, Waker},
};

use bytes::Bytes;

use crate::{
    error::StreamError,
    rr_bus::MaybeReady,
    stream::{StateFlags, waker::WakerSlot},
};

pub struct Outbound {
    data: Bytes,
    recv_window: u32,
    frame_limit: NonZeroU32,
    state: StateFlags,
    writer: WakerSlot,
}

impl Outbound {
    pub fn new(recv_window: u32, frame_limit: NonZeroU32) -> Self {
        Self {
            data: Default::default(),
            recv_window,
            frame_limit,
            state: Default::default(),
            writer: Default::default(),
        }
    }

    pub fn is_dead(&self) -> bool {
        self.state.fin_sent && self.state.fin_received
    }

    pub fn read_update(&mut self) -> (Bytes, bool) {
        match self.state {
            StateFlags { fin_sent: true, .. } => Default::default(),
            StateFlags {
                local_closed,
                fin_received: false,
                ..
            } => {
                let chunk = self
                    .recv_window
                    .min(self.frame_limit.get())
                    .min(u32::try_from(self.data.len()).unwrap_or(u32::MAX));
                self.recv_window -= chunk;
                let data = self.data.split_to(crate::safe_cast_usize(chunk));
                let fin_write = if self.data.is_empty() {
                    self.writer.wake();
                    if local_closed {
                        self.state.fin_sent = true;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                (data, fin_write)
            },
            StateFlags {
                fin_sent: false,
                fin_received: true,
                ..
            } => {
                self.state.fin_sent = true;
                (Default::default(), true)
            },
        }
    }

    pub fn received_window_update(&mut self, update: u32) -> Result<(), StreamError> {
        if update == 0 {
            return Ok(());
        }
        if self.state.fin_received {
            return Err("sent a non-empty WINDOW_UPDATE frame after FIN_READ".into());
        }
        self.recv_window = self
            .recv_window
            .checked_add(update)
            .ok_or("sent a WINDOW_UPDATE frame that would overflow the window")?;
        Ok(())
    }

    pub fn received_fin_read(&mut self) -> Result<(), StreamError> {
        if self.state.fin_received {
            return Err("sent duplicate FIN_READ".into());
        }
        self.state.fin_received = true;
        self.data = Default::default();
        self.writer.wake();
        Ok(())
    }

    pub fn poll_write_ready(&mut self, waker: &Waker) -> Poll<io::Result<()>> {
        match self.state {
            StateFlags {
                local_closed: false,
                fin_sent: false,
                fin_received: false,
            } => self.poll_flushed(waker).map(Ok),
            StateFlags { .. } => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
    }

    pub fn poll_flushed(&mut self, waker: &Waker) -> Poll<()> {
        if self.data.is_empty() {
            Poll::Ready(())
        } else {
            self.writer.register(waker);
            Poll::Pending
        }
    }

    pub fn write(&mut self, data: Bytes) -> io::Result<()> {
        match self.state {
            StateFlags {
                local_closed: false,
                fin_sent: false,
                fin_received: false,
            } => {
                if self.data.is_empty() {
                    self.data = data;
                    Ok(())
                } else {
                    Err(io::Error::other("sink not ready"))
                }
            },
            StateFlags { .. } => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    pub fn close_writing(&mut self) {
        self.state.local_closed = true;
    }
}

impl MaybeReady for Outbound {
    fn is_ready(&self) -> bool {
        match self.state {
            StateFlags {
                local_closed: false,
                fin_received: false,
                ..
            } => self.data.is_empty().not() && self.recv_window > 0,

            StateFlags {
                local_closed: true,
                fin_received: false,
                fin_sent: false,
            } => self.data.is_empty() || self.recv_window > 0,

            StateFlags {
                fin_received: true,
                fin_sent: false,
                ..
            } => true,

            StateFlags { .. } => false,
        }
    }
}

impl fmt::Debug for Outbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbound")
            .field("data_len", &self.data.len())
            .field("recv_window", &self.recv_window)
            .field("frame_limit", &self.frame_limit)
            .field("state", &self.state)
            .finish()
    }
}
