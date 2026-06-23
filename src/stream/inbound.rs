use std::{
    fmt,
    num::NonZeroU32,
    ops::Not,
    task::{Poll, Waker},
};

use crate::{
    buffer::{Data, DataList},
    error::StreamError,
    flow_control::{FlowControl, GlobalWindow},
    rr_bus::MaybeReady,
    stream::{StateFlags, waker::WakerSlot},
};

pub struct Inbound {
    data: DataList,
    remaining_window: u32,
    flow_control: FlowControl,
    state: StateFlags,
    reader: WakerSlot,
}

impl Inbound {
    pub fn new(recv_window: NonZeroU32, global_window: GlobalWindow) -> Self {
        Self {
            data: Default::default(),
            remaining_window: recv_window.get(),
            flow_control: global_window.flow_control(recv_window),
            state: Default::default(),
            reader: Default::default(),
        }
    }

    pub fn is_dead(&self) -> bool {
        self.state.fin_sent && self.state.fin_received
    }

    pub fn read_update(&mut self) -> (u32, bool) {
        match self.state {
            StateFlags {
                local_closed: false,
                ..
            } => {
                let update = self.flow_control.get_update();
                self.remaining_window += update;
                (update, false)
            },
            StateFlags {
                fin_sent: false,
                local_closed: true,
                ..
            } => {
                self.state.fin_sent = true;
                (0, true)
            },
            _ => Default::default(),
        }
    }

    pub fn received_data(&mut self, data: Data) -> Result<(), StreamError> {
        if data.as_ref().is_empty() {
            return Ok(());
        }
        if self.state.fin_received {
            return Err("sent a non-empty DATA frame after FIN_WRITE".into());
        }
        self.remaining_window = self
            .remaining_window
            .checked_sub(
                u32::try_from(data.as_ref().len())
                    .expect("data should contain at most one full frame"),
            )
            .ok_or("sent a DATA frame that would underflow the window")?;
        if self.state.local_closed.not() {
            self.data.push_back(data);
            self.reader.wake();
        }
        Ok(())
    }

    pub fn received_fin_write(&mut self) -> Result<(), StreamError> {
        if self.state.fin_received {
            return Err("sent a duplicate FIN_WRITE".into());
        }
        self.state.fin_received = true;
        self.flow_control.writing_closed();
        self.reader.wake();
        Ok(())
    }

    pub fn poll_read(&mut self, waker: &Waker) -> Poll<Option<Data>> {
        if let Some(data) = self.data.pop_front() {
            self.flow_control
                .consumed_bytes(u32::try_from(data.as_ref().len()).unwrap());
            return Poll::Ready(Some(data));
        }

        if self.state.fin_received {
            self.state.local_closed = true;
            self.flow_control.reading_closed();
            Poll::Ready(None)
        } else {
            self.reader.register(waker);
            Poll::Pending
        }
    }

    pub fn close_reading(&mut self) {
        self.state.local_closed = true;
        self.data = Default::default();
        self.flow_control.reading_closed();
    }
}

impl MaybeReady for Inbound {
    fn is_ready(&self) -> bool {
        self.state.fin_sent.not() && (self.state.local_closed || self.flow_control.has_update())
    }
}

impl fmt::Debug for Inbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inbound")
            .field("remaining_window", &self.remaining_window)
            .field("flow_control", &self.flow_control)
            .field("state", &self.state)
            .finish()
    }
}
