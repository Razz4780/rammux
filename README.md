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

When you need to handle multiple independent data streams within one byte stream transport,
and that transport is **not** prone to [HOL blocking](https://en.wikipedia.org/wiki/Head-of-line_blocking).
HOL blocking on the transport level can become a serious performance bottleneck.
Instead of running ramux over a single TCP connection, you should probably rather use multiple TCP connections
or QUIC.

## What this crate provides

- `RammuxConnection<IO>`: the protocol driver that owns the transport
- `RammuxDuplex`: a virtual bidirectional byte stream
- per-stream flow control with local receive window autotuning
- fair round-robin scheduling and data framing across ready streams
- graceful downgrade back to the original transport

The crate is transport-agnostic.
If it implements `AsyncRead + AsyncWrite + Unpin`, it can carry rammux.

## Mental model

This crate does not spawn background tasks. Your application keeps the connection
alive and working by manually polling the driver.

While you do that, the driver:

1. reads inbound frames,
2. writes outbound frames,
3. yields new inbound streams,
4. initiates and handles `PING` exchanges, and

Each accepted or created stream is represented as `RammuxDuplex`, which
implements:

- `futures::Sink<bytes::Bytes>` for writing
- `futures::Stream<Item = bytes::Bytes>` for reading

If the connection stops being polled, all stream IO stalls with it.

## Connection setup

rammux does not define an in-band handshake. Before running the protocol,
the application must agree on compatible configuration and roles.

[examples/negotiation.rs](https://github.com/Razz4780/rammux/blob/main/examples/negotiation.rs) shows an example out-of-band negotiation flow.

## Flow control

Each stream has two independent receive windows, one on each side.
As the writer sends bytes, it consumes the receive window on the other side.
As the reader reads bytes, the rammux driver automatically sends `WINDOW_UPDATE` frames
that restore the receive window. This lets the peer continue writing.

This implementation also maintains a local global receive window pool.
Streams that sustain high throughput can temporarily borrow from that pool so the peer
spends less time waiting for window updates. RTT sampled from `PING`
request/response frames is used to tune the receive window toward roughly
`1.5 * bandwidth-delay-product`.

Those tuning details are local behavior. On the wire, the peer only sees normal
`WINDOW_UPDATE` frames.

## Shutdown and transport recovery

rammux never shuts down the original transport.
The protocol always ends with a downgrade handshake that allows for reclaiming the transport.

## Examples

Usage examples live in the [examples directory](https://github.com/Razz4780/rammux/tree/main/examples):
- `examples/negotiation.rs`: negotiate config out of band
- `examples/flow_control.rs`: show blocked and active streams coexisting
- `examples/downgrade.rs`: orderly shutdown and transport recovery
- `examples/heavy_io.rs`: compare rammux against raw transport throughput

## Protocol

See [PROTOCOL.md](PROTOCOL.md) for the current on-wire format.

This crate does not promise backwards compatibility across major versions.
