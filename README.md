# rammux

[![crates.io](https://img.shields.io/crates/v/rammux.svg)](https://crates.io/crates/rammux)
[![Released API docs](https://docs.rs/rammux/badge.svg)](https://docs.rs/rammux)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![CI](https://github.com/Razz4780/rammux/actions/workflows/ci.yaml/badge.svg)](https://github.com/Razz4780/rammux/actions/workflows/ci.yaml)
[![MSRV](https://img.shields.io/crates/msrv/rammux)](https://crates.io/crates/rammux)

`rammux` is a stream multiplexer for Tokio. Two peers run many independent bidirectional byte streams
over one reliable byte stream transport, such as a TCP connection, and the streams share the bandwidth
of that transport fairly.

## Why rammux

Use it when you need many independent streams over one connection, and no stream should be able to starve
the others.

The mainstream multiplexers (HTTP/2, yamux) let the *receiver* tune how much each stream may send, and the
receiver cannot see what the sender has waiting. Depending on the implementation:

- the stream that comes first takes most of the bandwidth, and a stream that comes later is choked; or
- when the reader of a busy stream slows down, the other streams slow down with it; or
- every stream is capped by a fixed window, however idle the connection is.

In rammux the *sender* decides, for every frame, which stream may use the memory that the receiver offers.
Streams get equal shares, a latecomer catches up within a few round trips, and a stream whose reader is slow or
stalled leaves the bandwidth to the others. The cost is about 5% more packets on connections that carry data
in one direction only.

[TRANSIT_CONTROL.md](TRANSIT_CONTROL.md) explains how it works, with measurements against hyper and yamux.

## Quick start

```toml
[dependencies]
rammux = "1"
bytes = "1"
futures = "0.3"
tokio = { version = "1", features = ["full"] }
```

Two peers over an in-memory transport: the server echoes every stream, the client sends `hello` and reads it back.

```rust
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rammux::{
    config::{RammuxConfig, RammuxRole},
    connection::{RammuxConnection, RammuxProgress},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Any reliable byte stream can be the transport, for example a `TcpStream`.
    let (client_io, server_io) = tokio::io::duplex(4096);
    // The peers take opposite roles, and their configs must be compatible. The defaults are.
    let mut client = RammuxConnection::new(RammuxRole::Client, client_io, RammuxConfig::new())?;
    let mut server = RammuxConnection::new(RammuxRole::Server, server_io, RammuxConfig::new())?;

    // The server echoes every stream, until the client ends the session.
    let server = tokio::spawn(async move {
        loop {
            match server.progress().await? {
                RammuxProgress::Inbound(stream) => {
                    let (sink, stream) = stream.into_split();
                    tokio::spawn(stream.map(Ok).forward(sink));
                },
                // Completes the downgrade, and returns the transport.
                RammuxProgress::Downgraded(downgraded) => break downgraded.await,
                RammuxProgress::Empty => {},
            }
        }
    });

    // A stream is a `Sink` and a `Stream` of `Bytes`. It can move to another task.
    let mut stream = client
        .try_start_outbound()?
        .expect("the stream limit is not reached");
    let mut echo = tokio::spawn(async move {
        stream.send(Bytes::from_static(b"hello")).await?;
        Ok::<_, std::io::Error>(stream.next().await)
    });

    // Nothing runs in the background: streams make progress only while the connection is polled.
    let reply = loop {
        tokio::select! {
            reply = &mut echo => break reply??,
            progress = client.progress() => { progress?; },
        }
    };
    assert_eq!(reply.as_deref(), Some(&b"hello"[..]));

    // End the session, and take the transports back.
    let _client_io = client.downgrade()?.await?;
    let _server_io = server.await??;
    Ok(())
}
```

## How it works

### The connection

`RammuxConnection` owns the transport and runs the protocol on it. It does not spawn tasks, and nothing happens
in the background: your code polls `RammuxConnection::progress` in a loop. While it does, the connection

- moves the data of all streams,
- hands you the streams that the peer starts (`RammuxProgress::Inbound`),
- keeps the connection alive, and fails with an error when the peer goes silent.

If the connection is not polled, all of its streams stall.

### Streams

Either peer starts a stream with `RammuxConnection::try_start_outbound`, up to the configured limit.
A stream is a `RammuxDuplex`:

- a `futures::Sink<bytes::Bytes>` for writing,
- a `futures::Stream<Item = bytes::Bytes>` for reading.

It can move to another task, and `into_split` separates the two halves. The directions close independently:
closing or dropping the sink ends the stream for the peer's reader, and dropping the reader tells the peer's
writer to stop. Flushing the sink waits until the connection has taken the data, not until the peer has read it.

### Configuration

rammux has no handshake. Both peers create their `RammuxConfig` up front, with opposite roles
(`RammuxRole::Client` and `RammuxRole::Server`) and settings that match:

- `frame_limit` is the same on both sides,
- one side's `max_outbound_streams` does not exceed the other side's `max_inbound_streams`,
- one side's `remote_recv_window` and `remote_recv_pool` are the other side's `local_recv_window` and
  `local_recv_pool`.

Two default configs match each other. If the peers do not share their settings in advance, they exchange them before starting rammux:
[examples/negotiation.rs](https://github.com/Razz4780/rammux/blob/main/examples/negotiation.rs) shows how.

### Flow control

Every stream may have `local_recv_window` bytes waiting to be read (64 KiB by default), and all streams of a
connection share `local_recv_pool` bytes on top of that (4 MiB by default). The memory that a connection uses
for received data is bounded by these two.

The pool is also what limits the speed of the connection and the queue it builds: about pne BDP is enough for full speed, and a smaller pool keeps interactive streams more responsive
next to bulk transfers. See [TRANSIT_CONTROL.md](TRANSIT_CONTROL.md#choosing-the-settings).

### Keepalive

The connection sends a `PING` at least every `ping_interval` (5 s by default), and the peer answers. If nothing
arrives for `inbound_frame_timeout` (10 s by default), the connection fails with a timeout error.
The same exchange measures the round trip time, reported in `RammuxConnection::stats`.

### Shutdown

rammux never closes the transport. `RammuxConnection::downgrade` ends the session with a short exchange,
and returns the transport, ready for whatever comes next. The peer learns about it from
`RammuxProgress::Downgraded`, and gets its transport back the same way.

A downgrade ends all streams, so finish them first: data that the connection has not sent yet is lost.
Dropping the connection instead ends everything abruptly, and the peer sees an error.

## Requirements

- **Tokio with timers.** Create the connection inside a Tokio runtime that has the time driver enabled
  (`enable_time` or `enable_all`). It panics otherwise.
- **`TCP_NODELAY`.** Set it if the transport is a TCP socket. rammux sends small control frames, and Nagle's
  algorithm would hold them back.
- **A transport that takes a frame in one write.** rammux writes the header and the payload of a frame together.
  If your transport has no vectored writes, wrap it in `tokio::io::BufWriter`.

## Examples

The [examples directory](https://github.com/Razz4780/rammux/tree/main/examples) has runnable programs:

- `negotiation.rs`: agree on the config over the transport
- `flow_control.rs`: a blocked stream and an active stream side by side
- `downgrade.rs`: an orderly shutdown that returns the transport

## Documentation

- [API documentation](https://docs.rs/rammux)
- [PROTOCOL.md](PROTOCOL.md): the wire protocol
- [TRANSIT_CONTROL.md](TRANSIT_CONTROL.md): how streams share the connection, compared with HTTP/2 and yamux

A new major version of the crate may change the wire protocol: both peers must run the same major version.
