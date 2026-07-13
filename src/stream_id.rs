use std::fmt;

use rand::{
    Rng,
    distr::{Distribution, StandardUniform},
};

use crate::config::RammuxRole;

/// 24-bit ID of a rammux stream.
///
/// Each active rammux stream has an ID uniquely identifies that stream
/// relative to all other active streams. Freed IDs are reused.
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId(u32);

impl StreamId {
    /// Maximal value of a 24-bit unsigned integer.
    const MAX: u32 = u32::MAX >> 8;

    /// Returns the role that initiated the stream.
    pub const fn initiated_by(self) -> RammuxRole {
        if self.0.is_multiple_of(2) {
            RammuxRole::Client
        } else {
            RammuxRole::Server
        }
    }

    /// Reads an ID from the given big endian representation.
    pub(crate) const fn from_be_bytes(bytes: [u8; 3]) -> Self {
        let [b1, b2, b3] = bytes;
        Self(u32::from_be_bytes([0, b1, b2, b3]))
    }

    /// Returns a big endian representation of this ID.
    pub(crate) const fn to_be_bytes(self) -> [u8; 3] {
        let [_, b1, b2, b3] = self.0.to_be_bytes();
        [b1, b2, b3]
    }

    /// Returns a [`slab`] index for this ID.
    pub(crate) const fn slab_idx(self) -> usize {
        crate::safe_cast_usize(self.0 / 2)
    }

    /// Returns an ID for the index of its allocated [`slab`] slot
    /// and the [`RammuxRole`] that initiated the connection.
    pub(crate) fn from_slab_idx(idx: usize, role: RammuxRole) -> Option<Self> {
        let id = idx
            .checked_mul(2)?
            .checked_add(usize::from(role == RammuxRole::Server))?;
        let id = u32::try_from(id).ok()?;
        (id <= Self::MAX).then_some(Self(id))
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

impl Distribution<StreamId> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> StreamId {
        let num = rng.next_u32();
        StreamId(num >> 8)
    }
}

impl From<StreamId> for u32 {
    fn from(value: StreamId) -> Self {
        value.0
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use crate::{config::RammuxRole, stream_id::StreamId};

    #[rstest]
    #[case(0x000000, [0x00, 0x00, 0x00])]
    #[case(0x000001, [0x00, 0x00, 0x01])]
    #[case(0xAABBCC, [0xAA, 0xBB, 0xCC])]
    #[test]
    fn be_representation(#[case] raw_id: u32, #[case] expected: [u8; 3]) {
        assert!(raw_id <= StreamId::MAX);
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
    #[test]
    fn slab_idx(#[case] id: StreamId, #[case] idx: usize) {
        assert_eq!(id.slab_idx(), idx);
    }

    #[rstest]
    #[case(0, RammuxRole::Client, Some(StreamId(0)))]
    #[case(0, RammuxRole::Server, Some(StreamId(1)))]
    #[case(1, RammuxRole::Client, Some(StreamId(2)))]
    #[case(1, RammuxRole::Server, Some(StreamId(3)))]
    #[case(crate::safe_cast_usize(StreamId::MAX), RammuxRole::Client, None)]
    #[case(crate::safe_cast_usize(StreamId::MAX), RammuxRole::Server, None)]
    #[test]
    fn from_slab_idx(
        #[case] idx: usize,
        #[case] role: RammuxRole,
        #[case] expected: Option<StreamId>,
    ) {
        let calculated = StreamId::from_slab_idx(idx, role);
        assert_eq!(calculated, expected)
    }
}
