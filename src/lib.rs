//! Tokio-based asynchronous stream multiplexing over a single byte transport.
//!
//! Rammux lets two peers open many independent virtual byte streams inside one
//! reliable byte stream transport, for example a TCP (mind the head-of-line blocking issue)
//! or a UNIX connection.
//!
//! Each virtual stream is exposed as a [`RammuxDuplex`](stream::RammuxDuplex),
//! which implements both [`Sink`](futures::Sink) and [`Stream`](futures::Stream).
//! It also allows for graceful downgrade and recovery of the byte stream transport.
//!
//! The transport itself is driven by [`RammuxConnection`](connection::RammuxConnection).
//! That connection owns protocol IO, handles stream creation, applies per-stream flow
//! control, sends keepalive `PING`s, and performs graceful downgrade back to the
//! underlying transport when Rammux is finished.
//!
//! # Configuration and negotiation
//!
//! Rammux does not define an in-band handshake for transport parameters. Before
//! constructing both peers, the application must ensure that both sides use a compatible
//! [`RammuxConfig`](config::RammuxConfig) and complementary [`RammuxRole`](config::RammuxRole)s.
//!
//! See [examples/negotiation.rs](https://github.com/Razz4780/rammux/blob/main/examples/negotiation.rs)
//! for one way to negotiate config out of band.
//!
//! # Protocol
//!
//! See [PROTOCOL.md](https://github.com/Razz4780/rammux/blob/main/PROTOCOL.md).
//!
//! # Examples
//!
//! Usage examples live in the
//! [examples directory](https://github.com/Razz4780/rammux/tree/main/examples):
//!
//! - `negotiation.rs`: out-of-band config negotiation
//! - `flow_control.rs`: per-stream flow control and fairness
//! - `downgrade.rs`: graceful downgrade and transport recovery
//! - `heavy_io.rs`: heavier traffic and transport comparisons

#![deny(
    unused_crate_dependencies,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    missing_docs
)]

mod buffer;
mod codec;
pub mod config;
pub mod connection;
mod error;
mod flow_control;
mod header;
mod rr_bus;
pub mod stream;
mod stream_id;

pub use crate::{error::RammuxError, stream_id::StreamId};

static_assertions::const_assert!(std::mem::size_of::<u32>() <= std::mem::size_of::<usize>());

/// Convenience function for casting `u32` to `usize`.
///
/// The cast never truncates, as statically asserted above.
///
/// This trait exists only to avoid commenting in all of the places where this cast is used.
const fn safe_cast_usize(value: u32) -> usize {
    value as usize
}

// clap is used in examples.
#[cfg(test)]
use clap as _;
