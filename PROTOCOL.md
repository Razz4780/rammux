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
- the base receive window size each side grants to every virtual stream
- the size of the pool of receive window that each side shares between all its streams

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

The responder answers the latest `PING` it has read, as soon as possible, with at most one `PONG` per frame.
A `PING` that is superseded by a newer one before its answer is written gets no answer. The `PONG` contains:

- `t_echo`, copied from the `PING`,
- `t_read`, the responder's clock at the moment it read the header of the frame with that `PING`,
- `rx_bytes`, the responder's counter at the same moment. It does not include the payload of the frame
  that carried the `PING`.

`t_read` and `rx_bytes` are for the measurements of the sender of the `PING` (how much the peer received, and
when). The flow control of the streams does not use them, but `DelayControl` does (below), so they must be filled in
correctly.

Answering only the latest `PING` keeps the memory of the responder O(1), with no need to stop reading, and an
answer never waits in a queue behind older ones (that wait would be counted as delay in the sender's samples).
A sender must not rely on an answer to a `PING` if the transport does not drain, or to every `PING`. Every `PONG`
is an independent sample, and `rx_bytes` is cumulative. After the responder has sent `TERM`, `PING`s that arrive are
not answered.

The sender of the `PING` accepts a `PONG` whenever it comes. On receiving it at its clock `now`:

- `rtt = now - t_echo` is a round trip time measured on a single clock. The `PONG` is ignored for the
  purpose of this sample unless `t_echo` is strictly later than the `t_echo` of the last accepted `PONG`
  and not later than the newest `t_ping` the sender has sent. The responder answers only the latest `PING`
  it has read, so the echoes of a sound peer strictly increase; a duplicate, a reordered `PONG` and an echo
  of a `PING` that was never sent are therefore ignored. The age of `t_echo` is not limited: a `PONG` may
  wait in a queue for as long as the queue is.
- `now - t_read` is the time from "the peer read our `PING`" to "we got its answer", plus an unknown clock
  offset. Only differences between such values mean anything.

All arithmetic on timestamps and counters wraps; differences are read as `i32`.

A `PING` is sent after every 32 KiB of `DATA` written (`ping_bytes`), and at least every 5 seconds
(`ping_interval`) to keep the connection alive. Several can be in flight.

With `DelayControl` (see [TRANSIT_CONTROL.md](TRANSIT_CONTROL.md)), the `PONG`s are also the input of the connection
window: the one-way delay of the `PING`s (`t_read - t_echo`) is the queue signal, `rx_bytes` the delivered bytes. A `PING`
then goes out also after `W / 4` bytes of `DATA`, and at once when the window keeps a frame from being sent and `DATA`
was sent since the last `PING`. Both triggers need `DATA` written since the last `PING`, so a peer
sends at most one `PING` per `DATA` frame. These extra `PING`s can be superseded: the responder answers only the latest
`PING` it has read, and the sender takes whatever `PONG`s come back as independent samples.

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

The flow control is *inverted*: the receiver does not tune anything, and the sender decides how much of the
receiver's memory each stream uses. See [TRANSIT_CONTROL.md](TRANSIT_CONTROL.md) for the reasons.

Negotiated beforehand: the base window `B` of every stream, and the pool `P`: memory that all streams of the connection
share on top of their base windows, in each direction.

Each side tracks `unacked_s` for both directions of every stream: bytes sent minus credit received (sending),
bytes received minus credit sent (receiving). `borrowed_s = max(0, unacked_s - B)`. The sum of `borrowed_s` over the
streams of one direction is the use of the pool, `U`.

Rules:

1. the sender keeps `U <= P`, and sends at most as much as it is allowed to borrow (the policy is local);
2. the receiver counts the bytes it receives, and returns credit as the application reads. It must return
   credit no later than when the bytes of a stream that were read and not yet credited reach `B`: otherwise
   the sender can be left with a full window and no credit coming, forever. rammux sends `WINDOW(credit)` once
   the credit reaches `max(1, min(32 KiB, B / 2))`;
3. `FIN_READ` zeroes the counter of the stream on both sides, and the receiver discards all `DATA` that arrives
   after it was sent;
4. no credit comes after `FIN_READ`.

These are protocol violations:

- a `DATA` frame that would make `U_rx > P_rx`
- a `WINDOW` with more credit than the bytes that are unacknowledged on the stream
- a `WINDOW` after `FIN_READ` of the same peer

## Downgrade and transport recovery

Either side may leave rammux by sending `TERM`, which is a frame with only the `TERM` flag (two bytes).

The clean downgrade procedure is:

1. send `TERM`
2. continue reading rammux frames until the peer's `TERM` arrives
3. return the underlying transport

`TERM` is a session-level marker, not a stream-level one. Frames already in
flight can still arrive before the peer's final `TERM`, so a peer that starts
downgrade must keep draining the transport until the handshake completes.
