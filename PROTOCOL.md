# rammux protocol

This document describes the wire format and session rules implemented in rammux (protocol version 2).

rammux multiplexes many virtual bidirectional byte streams over one ordered byte
transport. The underlying transport is expected to be a reliable full-duplex
byte stream. It must not delay small writes: set `TCP_NODELAY` on TCP sockets.
Control frames (credit, `PING`s) are small, and Nagle's algorithm together with delayed ACKs
holds them back for tens of milliseconds.

Inspired by [Yamux](https://github.com/hashicorp/yamux).

## Session preconditions

Before the first rammux frame is sent, the peers must already agree on:

- the version of this protocol (2): frames carry no version, so peers of different versions cannot tell,
  and they misparse each other's frames (version 1 has another header)
- which side is the `client` and which side is the `server`
- the maximum frame payload size (`frame_limit`, at most 65 535)
- how many concurrent inbound streams each side is willing to serve (at most 32 768)
- the initial receive window size each side grants to a new virtual stream

## Stream IDs

Each virtual stream has a 16-bit unsigned integer ID,
unique relative to all other active streams.

- client-initiated streams use even IDs
- server-initiated streams use odd IDs

So each side has 32 768 IDs.
A side may reuse an ID once the stream is closed in both directions on both sides: it has sent and received
both `FIN_WRITE` and `FIN_READ` for it. Until then the stream occupies its ID and one slot of the
limit on the inbound streams of the side that did not start it. A frame with `SYN` for an ID that is still
open is a protocol violation.

## Frame format

There is one frame type. All integers are big-endian.

```
[flags: u16][header fields, in wire order, each present iff its flag is set][payload]
```

| Bit | Flag | Fields added | Meaning |
| --- | --- | --- | --- |
| `0x0001` | `SYN` | `stream_id`* | first frame of a stream |
| `0x0002` | `FIN_READ` | `stream_id`* | the sender of this frame stopped reading the stream |
| `0x0004` | `FIN_WRITE` | `stream_id`* | the sender of this frame stopped writing the stream |
| `0x0008` | `WINDOW` | `stream_id`*, `credit: u32` | bytes returned to the peer's transmit window on this stream, at least 1 |
| `0x0010` | `DATA` | `stream_id`*, `len: u16`, then `len` payload bytes | stream data, `1 <= len <= frame_limit` |
| `0x0020` | `PING` | `t_ping: u32` | the sender's clock in microseconds, set when the frame is written |
| `0x0040` | `PONG` | `t_echo: u32`, `t_read: u32`, `rx_bytes: u32` | answer to a `PING` |
| `0x0080` | reserved | none | must be zero |
| `0x0100` | `TERM` | none | end of the rammux session (downgrade) |

\* `stream_id` is present iff at least one of `SYN`, `FIN_READ`, `FIN_WRITE`, `WINDOW`, `DATA` is set,
and it appears once.

Wire order of the fields: `stream_id`, `credit`, `len`, `t_ping`, `t_echo`, `t_read`, `rx_bytes`, then the
payload. The longest header is 26 bytes.

Stream flags, `WINDOW` and `DATA` can be combined in one frame. `PING` and `PONG` can ride on any frame, with or
without a stream part, but a frame carries at most one of each. `TERM` cannot be combined with anything.

The parts of one frame are processed in this order:
`PONG`, `PING` (the connection part), then `SYN`, `WINDOW`, `DATA`, `FIN_WRITE`, `FIN_READ`
(the stream part).

Everything that must reach the peer quickly is written in this order:
`PONG`, `PING`, stream control, stream data. The header and the payload of a frame should reach the transport
together (a TCP transport would otherwise put the header in a segment of its own).

### Violations

The following are protocol violations:

- reserved bits (`0xFE80`) set, a frame without flags, `TERM` combined with other flags
- `DATA` with `len = 0` or `len > frame_limit`
- `WINDOW` with `credit = 0`
- the stream violations described below

## PING and PONG

`PING`s and `PONG`s have no identity and need no state.

Each side counts microseconds since its own epoch in a `u32` private clock (it wraps every 71.6 minutes).
`t_ping` is the value of that clock when the first byte of the frame is accepted by the transport.
Successive `t_ping`s of one side strictly increase.

Each side also keeps two `u32` counters, `tx_bytes` and `rx_bytes`, which wrap.
They count the payload bytes of `DATA` frames written and read (including the data discarded by the reader).

The responder answers every `PING` exactly once, in order, as soon as possible, with at most one `PONG`
per frame. The `PONG` contains:

- `t_echo`, copied from the `PING`,
- `t_read`, the responder's clock at the moment it read the header of the frame with that `PING`,
- `rx_bytes`, the responder's counter at the same moment. It does not include the payload of the frame
  that carried the `PING`.

`t_read` and `rx_bytes` are for the measurements of the sender of the `PING` (how much the peer received, and
when). The flow control of this version does not use them, but they must be filled in correctly.

A responder may stop reading while it owes too many `PONG`s (rammux stops at 256). That bounds the memory a
flood of `PING`s can take, and pushes back on the sender of the `PING`s through the transport. A sender
must not rely on an answer to a `PING` if the transport does not drain. After the responder has sent `TERM`,
`PING`s that arrive are not answered.

The sender of the `PING` accepts a `PONG` whenever it comes. On receiving it at its clock `now`:

- `rtt = now - t_echo` is a round trip time measured on a single clock. It is ignored if it comes out
  negative.
- `now - t_read` is the time from "the peer read our `PING`" to "we got its answer", plus an unknown clock
  offset. Only differences between such values mean anything.

All arithmetic on timestamps and counters wraps; differences are read as `i32`.

A `PING` is sent after every 32 KiB of `DATA` written (`ping_bytes`), and at least every 5 seconds
(`ping_interval`) to keep the connection alive. Several can be in flight.

## Stream lifecycle

### Opening a stream

The first frame for a stream ID must carry `SYN`.
It rides on the first frame the stream sends: it can carry `DATA`, `WINDOW`, `FIN_WRITE`, or `FIN_READ`.
A stream that was opened but has nothing to send yet sends nothing.

Opening a stream without `SYN`, opening an already active ID, using an ID from
the wrong parity pool, or exceeding the peer's inbound-stream limit is a
protocol violation, and so is a frame for a stream that is not open.

### Independent read and write closure

Each virtual stream is duplex, so the two directions close independently.

`FIN_WRITE` means: "I will not send more data on this stream."
Sending `DATA` or another `FIN_WRITE` after `FIN_WRITE` is a protocol violation.

`FIN_READ` means: "I am no longer reading data from this stream."
Sending `WINDOW` or another `FIN_READ` after `FIN_READ` is a protocol violation.

## Flow control

Each stream has two independent receive windows, one on each side.
At all times, a receive window size must fit into a 32-bit unsigned integer.

Rules:

1. the sender must not transmit more data than the currently available window
2. each `DATA` payload decrements that available window
3. each `WINDOW` with `credit` increments it by `credit`

This implementation treats the following as protocol violations:

- a `DATA` frame that would underflow the remaining receive window
- a `WINDOW` that would overflow the sender-side window accounting

The window of a stream is autotuned by its receiver, which does not show on the wire.

## Downgrade and transport recovery

Either side may leave rammux by sending `TERM`, which is a frame with only the `TERM` flag (two bytes).

The clean downgrade procedure is:

1. send `TERM`
2. continue reading rammux frames until the peer's `TERM` arrives
3. return the underlying transport

`TERM` is a session-level marker, not a stream-level one. Frames already in
flight can still arrive before the peer's final `TERM`, so a peer that starts
downgrade must keep draining the transport until the handshake completes.
