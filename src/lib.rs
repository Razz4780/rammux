//! Tokio-based asynchronous stream multiplexing over a single byte transport.
//!
//! rammux lets two peers open many independent virtual byte streams inside one
//! reliable byte stream transport, for example a TCP or a UNIX connection.
//!
//! Each virtual stream is exposed as a [`RammuxDuplex`](stream::RammuxDuplex),
//! which implements both [`Sink`](futures::Sink) and [`Stream`](futures::Stream).
//! It also allows for graceful downgrade and recovery of the byte stream transport.
//!
//! The transport itself is driven by [`RammuxConnection`](connection::RammuxConnection).
//! That connection owns protocol IO, handles stream creation, applies per-stream flow
//! control, sends keepalive `PING`s, and performs graceful downgrade back to the
//! underlying transport when rammux is finished.
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
//! # Runtime
//!
//! rammux runs on Tokio. [`RammuxConnection::new`](connection::RammuxConnection::new) must be called
//! within a Tokio runtime that has the time driver enabled, or it panics.
//!
//! # Configuration and negotiation
//!
//! rammux does not define an in-band handshake for transport parameters. Before
//! constructing both peers, the application must ensure that both sides use a compatible
//! [`RammuxConfig`](config::RammuxConfig) and complementary [`RammuxRole`](config::RammuxRole)s.
//!
//! See [examples/negotiation.rs](https://github.com/Razz4780/rammux/blob/main/examples/negotiation.rs)
//! for one way to negotiate config out of band.
//!
//! # Protocol
//!
//! See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md).
//! The rules by which the sender decides how much data to put on the path are in
//! [TRANSIT_CONTROL.md](https://github.com/Razz4780/rammux/blob/main/TRANSIT_CONTROL.md).
//!
//! Set `TCP_NODELAY` on the transport if it is a TCP socket: rammux sends small control frames.
//!
//! # Examples
//!
//! Usage examples live in the
//! [examples directory](https://github.com/Razz4780/rammux/tree/main/examples):
//!
//! - `negotiation.rs`: out-of-band config negotiation
//! - `flow_control.rs`: per-stream flow control and fairness
//! - `downgrade.rs`: graceful downgrade and transport recovery

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

/// The public types can move to another thread if the transport can, and can be shared between threads
/// if the transport can. Applications rely on that to spawn the connection and the streams as separate tasks,
/// so it is checked here, where a change that breaks it fails to compile.
const _: () = {
    use crate::{
        config::{ConfigError, RammuxConfig},
        connection::{Downgraded, RammuxConnection, RammuxProgress, RammuxStats},
        stream::{RammuxDuplex, RammuxSink, RammuxStream},
    };

    const fn send<T: Send>() {}
    const fn sync<T: Sync>() {}

    #[allow(dead_code)]
    const fn with_send_transport<IO: Send>() {
        send::<RammuxConnection<IO>>();
        send::<RammuxProgress<IO>>();
        send::<Downgraded<IO>>();
    }

    #[allow(dead_code)]
    const fn with_sync_transport<IO: Sync>() {
        sync::<RammuxConnection<IO>>();
        sync::<RammuxProgress<IO>>();
        sync::<Downgraded<IO>>();
    }

    send::<(RammuxDuplex, RammuxSink, RammuxStream)>();
    sync::<(RammuxDuplex, RammuxSink, RammuxStream)>();
    send::<(
        RammuxError,
        StreamId,
        RammuxConfig,
        ConfigError,
        RammuxStats,
    )>();
    sync::<(
        RammuxError,
        StreamId,
        RammuxConfig,
        ConfigError,
        RammuxStats,
    )>();
};

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
