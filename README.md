# rammux

[![crates.io](https://img.shields.io/crates/v/rammux.svg)](https://crates.io/crates/rammux)
[![Released API docs](https://docs.rs/rammux/badge.svg)](https://docs.rs/rammux)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![CI](https://github.com/Razz4780/rammux/actions/workflows/ci.yaml/badge.svg)](https://github.com/Razz4780/rammux/actions/workflows/ci.yaml)
[![MSRV](https://img.shields.io/crates/msrv/rammux)](https://crates.io/crates/rammux)

`rammux` is a Tokio-based stream multiplexer for reliable byte stream transports.
It lets two peers run many independent bidirectional byte streams over one
`AsyncRead + AsyncWrite` connection while keeping stream lifecycle, per-stream flow
control, keepalive, and shutdown in one place.

## When would you want to use it?

When you need many independent streams over one connection, and you want them to share its bandwidth fairly.

The mainstream TCP multiplexers that autotune receive windows all split the available bandwidth between their streams unfairly, to some extent:

- the stream that comes first takes most of the bandwidth, and a stream that comes later is choked; OR
- on an asymmetrical connection, within a single stream, the faster direction chokes the slower one; OR
- when the reader of a busy stream slows down, the bandwidth that the stream no longer uses does not go to the other streams.

These multiplexers tune the windows at the receiver. rammux fixes the issue by inverting the autotuning: it is the *sender* that decides which stream gets more throughput, and it decides with high granularity, for every frame (see [Flow control](#flow-control)). The cost: about 5% more packets on connections that only transfer data in one direction.

## What this crate provides

- `RammuxConnection<IO>`: the protocol driver that owns the transport
- `RammuxDuplex`: a virtual bidirectional byte stream
- per-stream flow control with a shared pool, fair shares and protection from stalled readers
- `PING`/`PONG` round trip time measurement
- fair round-robin scheduling and data framing across ready streams
- graceful downgrade back to the original transport

The crate is transport-agnostic. If the type implements async reads and writes, it can carry rammux.

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

## Mental model

This crate does not spawn background tasks. Your application keeps the connection
alive and working by manually polling the driver.

While you do that, the driver:

1. reads inbound frames,
2. writes outbound frames,
3. yields new inbound streams,
4. initiates and handles `PING` exchanges.

Each accepted or created stream is represented as `RammuxDuplex`, which
implements:

- `futures::Sink<bytes::Bytes>` for writing
- `futures::Stream<Item = bytes::Bytes>` for reading

If the connection stops being polled, all stream IO stalls with it.

## Connection setup

Create the connection within a Tokio runtime that has the time driver enabled (`enable_time` or `enable_all`):
rammux uses Tokio timers, and panics without them.

Set `TCP_NODELAY` on the transport if it is a TCP socket. rammux sends small control frames
(window credit, `PING`s), and Nagle's algorithm and delayed ACKs would hold them back.

rammux writes each frame (its header and its payload) with one vectored write. A transport that does not
implement vectored writes natively (`is_write_vectored()` is false) gets two writes per frame, the header and then the
payload, which costs a system call or a packet more. Wrap such a transport in a buffered writer
(`tokio::io::BufWriter`, or `BufStream` for both directions): rammux flushes after the last frame it has ready,
so nothing is held back. A buffered reader is fine too: its buffered bytes stay in the transport that the
downgrade returns.

rammux does not define an in-band handshake. Before running the protocol,
the application must agree on compatible configuration and roles.

[examples/negotiation.rs](https://github.com/Razz4780/rammux/blob/main/examples/negotiation.rs) shows an example out-of-band negotiation flow.

## Flow control

Every stream has a fixed base receive window, and all streams of a connection share a pool of receive window on top
of that. As the reader reads bytes, the rammux driver sends `WINDOW` frames that return credit to the writer.
The receiver does not tune anything. The *sender* decides how much of the peer's pool each stream uses: in proportion
to the rate its reader has proven (derived from the credit it gets back and the round trip time measured with
`PING`/`PONG`), and in turns when streams compete for the pool. A stalled reader stops borrowing, and
busy streams that write at least `frame_limit` bytes at a time converge to equal shares. A turn moves
one frame, and a frame holds at most one write, so a stream that writes less gets a proportionally smaller share.

A new or idle stream also gets an *advance*: a burst of unproven window (`advance_grant`, repaid from the credit that
comes back) so that it does not wait a loop per doubling. It is on by default, a quarter of the pool for one stream
and a quarter for all streams together (1 MiB each at the default 4 MiB pool); set `advance_grant` and
`advance_budget` to 0 to turn it off. Measured on netem: on a busy connection a 2 MiB burst finishes in 159 ms
instead of 930 ms and latecomers reach 40% of the link 2 to 3.7 times faster; slow or stalled readers cost nothing
extra, and readers that never read pin at most the budget. After idle, TCP's restart window caps the gain above
about 256 KiB.

See [TRANSIT_CONTROL.md](TRANSIT_CONTROL.md) for the rules, the reasons, and the known limits.
The connection fills the pool when there is enough data, like the autotuning multiplexers do,
so the pool bounds the queue that the connection builds on the path.
Size it for the latency you can accept.

## Shutdown and transport recovery

rammux never shuts down the original transport.
The protocol always ends with a downgrade handshake that allows for reclaiming the transport.

## Examples

Usage examples live in the [examples directory](https://github.com/Razz4780/rammux/tree/main/examples):
- `examples/negotiation.rs`: negotiate config out of band
- `examples/flow_control.rs`: show blocked and active streams coexisting
- `examples/downgrade.rs`: orderly shutdown and transport recovery

## Protocol

See [PROTOCOL.md](PROTOCOL.md) for the current on-wire format.

This crate does not promise backwards compatibility across major versions:
peers must run the same major version. See [CHANGELOG.md](CHANGELOG.md) for what changed.
