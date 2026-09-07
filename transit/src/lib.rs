//! A transit window: a limit on how much data is in flight, for minimum
//! latency at maximum bandwidth.
//!
//! [`Transit`] wraps any [`AsyncRead`](tokio::io::AsyncRead) +
//! [`AsyncWrite`](tokio::io::AsyncWrite) - a TCP socket, in practice - and
//! reads and writes payload through it transparently. There are no logical
//! streams and no multiplexing; the one thing it does is bound how much the
//! application has handed to the network at once, and steer that bound from
//! the queuing delay it observes.
//!
//! ```
//! use transit::{Config, Role, Transit};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> std::io::Result<()> {
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//!
//! // Any duplex transport will do; a socket pair stands in for TCP here.
//! let (near, far) = tokio::io::duplex(64 * 1024);
//! let mut client = Transit::new(near, Config::new(Role::Initiator));
//! let mut server = Transit::new(far, Config::new(Role::Responder));
//!
//! let writer = tokio::spawn(async move {
//!     client.write_all(b"payload").await?;
//!     client.shutdown().await
//! });
//! let mut received = Vec::new();
//! server.read_to_end(&mut received).await?;
//! writer.await??;
//!
//! assert_eq!(received, b"payload");
//! # Ok(())
//! # }
//! ```
//!
//! # What the window actually controls
//!
//! Running over TCP it does not control the network queue - TCP's congestion
//! controller already does. What it controls is how many bytes the application
//! has handed to the network stack between the peers, kernel socket buffers
//! and any proxies included.
//!
//! That is where the latency comes from. A writer that pushes megabytes into a
//! socket buffer puts them *ahead* of everything written later, and those bytes
//! leave at link rate whatever this protocol does. The window's job is to keep
//! that queue shallow while still handing TCP enough to keep its own congestion
//! window fed. The target is about **one** bandwidth-delay product: below it TCP
//! starves and throughput drops, above it the excess is standing queue and shows
//! up in latency one for one, at `(W - BDP) / rate`.
//!
//! A fixed bound cannot do that job, because the product varies by orders of
//! magnitude across paths - which is why the window is steered rather than
//! configured, by a delay-targeting rule after LEDBAT. [`window`] explains how,
//! and why each constant is what it is.
//!
//! # Two contracts a caller has to keep
//!
//! * **Both ends must run [`Transit`].** It is a framed protocol, not a
//!   transparent shim; a peer speaking raw bytes will fail to parse.
//! * **Exactly one end is the [`Role::Initiator`].** That end schedules the
//!   link-clearing probe both ends size from. Two initiators is a configuration
//!   error the connection reports rather than hanging on - see [`probe`].
//!
//! Beyond that the two ends are independent: each announces its own window, so
//! they need not be configured alike.
//!
//! # Where to look
//!
//! * [`connection`] - framing, the sender's credit, and the poll loop.
//! * [`window`] - how the granted window is sized, and the credit returned for
//!   it. Every tuned constant is there, with the measurement that set it.
//! * [`delay`] - turning arrival timestamps into a one-way queuing delay.
//! * [`probe`] - measuring a round trip no queue can inflate.

pub mod connection;
pub mod delay;
pub mod probe;
pub mod window;

mod frame;
mod rate;

pub use crate::{
    connection::{Config, DEFAULT_PROBE_SPACING, Stats, Transit},
    frame::MAX_PAYLOAD,
    probe::Role,
    window::{Growth, Sizing},
};
