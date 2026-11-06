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

This crate does not promise backwards compatibility across major versions.
