//! The wire format of rammux, see `PROTOCOL.md`.
//!
//! - [`header`]: the frame header, the only frame type there is,
//! - [`codec`]: reading and writing whole frames on the transport,
//! - [`clock`]: the private clock that stamps the `PING`s.
//!
//! Nothing here knows about the state of the connection or of the streams.

pub mod clock;
pub mod codec;
pub mod header;
