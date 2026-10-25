use std::fmt;

use crate::config::RammuxRole;

/// 16-bit ID of a rammux stream.
///
/// Each active rammux stream has an ID uniquely identifies that stream
/// relative to all other active streams. Freed IDs are reused.
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId(u16);

impl StreamId {
    /// Returns the role that initiated the stream.
    pub const fn initiated_by(self) -> RammuxRole {
        if self.0.is_multiple_of(2) {
            RammuxRole::Client
        } else {
            RammuxRole::Server
        }
    }

    /// Reads an ID from the given big endian representation.
    pub(crate) const fn from_be_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_be_bytes(bytes))
    }

    /// Returns a big endian representation of this ID.
    pub(crate) const fn to_be_bytes(self) -> [u8; 2] {
        self.0.to_be_bytes()
    }

    /// Returns a [`slab`] index for this ID.
    pub(crate) const fn slab_idx(self) -> usize {
        (self.0 / 2) as usize
    }

    /// Returns an ID for the index of its allocated [`slab`] slot
    /// and the [`RammuxRole`] that initiated the connection.
    pub(crate) fn from_slab_idx(idx: usize, role: RammuxRole) -> Option<Self> {
        let id = idx
            .checked_mul(2)?
            .checked_add(usize::from(role == RammuxRole::Server))?;
        u16::try_from(id).ok().map(Self)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:#08x}", self.initiated_by(), self.0 / 2)
    }
}

impl fmt::Debug for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use crate::{config::RammuxRole, stream_id::StreamId};

    #[rstest]
    #[case(0x0000, [0x00, 0x00])]
    #[case(0x0001, [0x00, 0x01])]
    #[case(0xAABB, [0xAA, 0xBB])]
    #[test]
    fn be_representation(#[case] raw_id: u16, #[case] expected: [u8; 2]) {
        let id = StreamId(raw_id);
        assert_eq!(id.to_be_bytes(), expected,);
        let reconstructed = StreamId::from_be_bytes(expected);
        assert_eq!(id, reconstructed);
    }

    #[rstest]
    #[case(StreamId(0), 0)]
    #[case(StreamId(1), 0)]
    #[case(StreamId(2), 1)]
    #[case(StreamId(3), 1)]
    #[case(StreamId(u16::MAX), 32767)]
    #[test]
    fn slab_idx(#[case] id: StreamId, #[case] idx: usize) {
        assert_eq!(id.slab_idx(), idx);
    }

    #[rstest]
    #[case(0, RammuxRole::Client, Some(StreamId(0)))]
    #[case(0, RammuxRole::Server, Some(StreamId(1)))]
    #[case(1, RammuxRole::Client, Some(StreamId(2)))]
    #[case(1, RammuxRole::Server, Some(StreamId(3)))]
    #[case(32767, RammuxRole::Client, Some(StreamId(u16::MAX - 1)))]
    #[case(32767, RammuxRole::Server, Some(StreamId(u16::MAX)))]
    #[case(32768, RammuxRole::Client, None)]
    #[case(32768, RammuxRole::Server, None)]
    #[test]
    fn from_slab_idx(
        #[case] idx: usize,
        #[case] role: RammuxRole,
        #[case] expected: Option<StreamId>,
    ) {
        let calculated = StreamId::from_slab_idx(idx, role);
        assert_eq!(calculated, expected)
    }

    #[test]
    fn initiator() {
        assert_eq!(StreamId(4).initiated_by(), RammuxRole::Client);
        assert_eq!(StreamId(5).initiated_by(), RammuxRole::Server);
    }
}
