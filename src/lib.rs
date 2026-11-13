//! A stream multiplexer for Tokio.
//!
//! rammux lets two peers run many independent bidirectional byte streams over one reliable
//! byte stream transport, for example a TCP or a UNIX connection. The streams share the bandwidth
//! of the transport fairly: a stream that comes late gets its share, and a stream whose reader is slow
//! does not slow down the others.
//!
//! - [`RammuxConnection`](connection::RammuxConnection) owns the transport and runs the protocol.
//!   Nothing happens in the background: your code polls it, and all streams stall when it does not.
//! - [`RammuxDuplex`](stream::RammuxDuplex) is one stream: a [`Sink`](futures::Sink) and a
//!   [`Stream`](futures::Stream) of [`Bytes`](bytes::Bytes).
//! - [`RammuxConfig`](config::RammuxConfig) holds the settings, which the two peers have to agree on.
//! - When the peers are done, [`downgrade`](connection::RammuxConnection::downgrade) ends the session
//!   and gives the transport back.
//!
//! # Quick start
//!
//! ```
//! use bytes::Bytes;
//! use futures::{SinkExt, StreamExt};
//! use rammux::{
//!     config::{RammuxConfig, RammuxRole},
//!     connection::{RammuxConnection, RammuxProgress},
//! };
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Any reliable byte stream can be the transport, for example a `TcpStream`.
//! let (client_io, server_io) = tokio::io::duplex(4096);
//! // The peers take opposite roles, and their configs must be compatible. The defaults are.
//! let mut client = RammuxConnection::new(RammuxRole::Client, client_io, RammuxConfig::new())?;
//! let mut server = RammuxConnection::new(RammuxRole::Server, server_io, RammuxConfig::new())?;
//!
//! // The server echoes every stream, until the client ends the session.
//! let server = tokio::spawn(async move {
//!     loop {
//!         match server.progress().await? {
//!             RammuxProgress::Inbound(stream) => {
//!                 let (sink, stream) = stream.into_split();
//!                 tokio::spawn(stream.map(Ok).forward(sink));
//!             },
//!             // Completes the downgrade, and returns the transport.
//!             RammuxProgress::Downgraded(downgraded) => break downgraded.await,
//!             RammuxProgress::Empty => {},
//!         }
//!     }
//! });
//!
//! // A stream is a `Sink` and a `Stream` of `Bytes`. It can move to another task.
//! let mut stream = client
//!     .try_start_outbound()?
//!     .expect("the stream limit is not reached");
//! let mut echo = tokio::spawn(async move {
//!     stream.send(Bytes::from_static(b"hello")).await?;
//!     Ok::<_, std::io::Error>(stream.next().await)
//! });
//!
//! // Nothing runs in the background: streams make progress only while the connection is polled.
//! let reply = loop {
//!     tokio::select! {
//!         reply = &mut echo => break reply??,
//!         progress = client.progress() => { progress?; },
//!     }
//! };
//! assert_eq!(reply.as_deref(), Some(&b"hello"[..]));
//!
//! // End the session, and take the transports back.
//! let _client_io = client.downgrade()?.await?;
//! let _server_io = server.await??;
//! # Ok(())
//! # }
//! ```
//!
//! # Requirements
//!
//! - Create the connection within a Tokio runtime that has the time driver enabled.
//!   [`RammuxConnection::new`](connection::RammuxConnection::new) panics otherwise.
//! - Set `TCP_NODELAY` if the transport is a TCP socket. rammux sends small control frames,
//!   and Nagle's algorithm would hold them back.
//! - If the transport has no vectored writes, wrap it in a `tokio::io::BufWriter`,
//!   so that the header and the payload of a frame leave together.
//!
//! # Configuration
//!
//! rammux has no handshake. The peers take opposite [`RammuxRole`](config::RammuxRole)s, and
//! their [`RammuxConfig`](config::RammuxConfig)s have to match: the docs of the config say which
//! settings, and how. Two default configs match each other.
//!
//! If the peers cannot know each other's settings in advance, they exchange them over the transport
//! before they start rammux, as in
//! [examples/negotiation.rs](https://github.com/Razz4780/rammux/blob/main/examples/negotiation.rs).
//!
//! # More
//!
//! - [Examples](https://github.com/Razz4780/rammux/tree/main/examples): config negotiation,
//!   flow control, downgrade.
//! - [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md): the wire protocol.
//! - [TRANSIT_CONTROL.md](https://github.com/Razz4780/rammux/blob/main/TRANSIT_CONTROL.md):
//!   how streams share the connection, compared with HTTP/2 and yamux.

#![deny(
    unused_crate_dependencies,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::undocumented_unsafe_blocks,
    missing_debug_implementations,
    missing_docs
)]

/// Compiles and runs the code in `README.md` with the doctests, so that it cannot rot.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

mod buffer;
pub mod config;
pub mod connection;
mod error;
mod flow;
mod pin_list;
pub mod stream;
mod stream_id;
mod wire;

use std::time::Duration;

use tokio::time::Instant;

pub use crate::{error::RammuxError, stream_id::StreamId};

static_assertions::const_assert!(std::mem::size_of::<u32>() <= std::mem::size_of::<usize>());

/// Convenience function for casting `u32` to `usize`.
///
/// The cast never truncates, as statically asserted above.
///
/// It exists only to avoid commenting in all of the places where this cast is used.
const fn safe_cast_usize(value: u32) -> usize {
    value as usize
}

/// Returns the instant `after` from now.
///
/// A duration too long to add, such as [`Duration::MAX`], means "never":
/// the deadline is then 30 years from now, as with [`tokio::time::sleep`].
fn deadline(after: Duration) -> Instant {
    const NEVER: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);
    let now = Instant::now();
    now.checked_add(after.min(NEVER)).unwrap_or(now + NEVER)
}
