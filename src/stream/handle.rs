use bytes::Bytes;

use crate::{
    StreamId,
    buffer::Data,
    codec::encoder::EncoderItem,
    error::StreamError,
    header::ControlFlags,
    rr_bus::MaybeReady,
    stream::{inbound::Inbound, outbound::Outbound},
};

#[derive(Debug)]
pub struct StreamHandle {
    pub(super) id: StreamId,
    pub(super) syn: bool,
    pub(super) inbound: Inbound,
    pub(super) outbound: Outbound,
}

impl StreamHandle {
    pub fn is_dead(&self) -> bool {
        self.inbound.is_dead() && self.outbound.is_dead()
    }

    pub fn read_update(&mut self) -> StreamUpdate {
        let (window_update, fin_read) = self.inbound.read_update();
        let (data, fin_write) = self.outbound.read_update();

        StreamUpdate {
            id: self.id,
            flags: ControlFlags {
                fin_read,
                fin_write,
                syn: std::mem::take(&mut self.syn),
            },
            window_update,
            data,
        }
    }

    pub fn received_data(
        &mut self,
        data: Data,
        fin_read: bool,
        fin_write: bool,
    ) -> Result<(), StreamError> {
        self.inbound.received_data(data)?;
        if fin_read {
            self.outbound.received_fin_read()?;
        }
        if fin_write {
            self.inbound.received_fin_write()?;
        }
        Ok(())
    }

    pub fn received_window_update(
        &mut self,
        update: u32,
        fin_read: bool,
        fin_write: bool,
    ) -> Result<(), StreamError> {
        self.outbound.received_window_update(update)?;
        if fin_read {
            self.outbound.received_fin_read()?;
        }
        if fin_write {
            self.inbound.received_fin_write()?;
        }
        Ok(())
    }

    pub fn abort(&mut self) {
        let _ = self.outbound.received_fin_read();
        let _ = self.inbound.received_fin_write();
    }
}

impl MaybeReady for StreamHandle {
    fn is_ready(&self) -> bool {
        self.inbound.is_ready() || self.outbound.is_ready()
    }
}

/// An update yielded by a Rammux stream.
///
/// Translates to one or two frames and can be converted to an [`EncoderItem`].
pub struct StreamUpdate {
    pub id: StreamId,
    pub flags: ControlFlags,
    pub window_update: u32,
    /// Cannot be larger than the configured frame limit.
    pub data: Bytes,
}

impl From<StreamUpdate> for EncoderItem {
    fn from(value: StreamUpdate) -> Self {
        if value.data.is_empty() {
            // Single WINDOW_UPDATE frame.
            EncoderItem::new_window_update(value.id, value.flags, value.window_update)
        } else if value.window_update > 0 {
            // Two frames, WINDOW_UPDATE and DATA.
            EncoderItem::new_window_update_and_data(
                value.id,
                value.flags,
                value.window_update,
                value.data,
            )
        } else {
            // Single DATA frame.
            EncoderItem::new_data(value.id, value.flags, value.data)
        }
    }
}
