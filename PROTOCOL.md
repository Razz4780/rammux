# eammux protocol

This document describes the wire format and session rules implemented in rammux.

rammux multiplexes many virtual bidirectional byte streams over one ordered byte
transport. The underlying transport is expected to be a reliable full-duplex
byte stream.

Inspired by [Yamux](https://github.com/hashicorp/yamux).

## Session preconditions

Before the first rammux frame is sent, the peers must already agree on:

- which side is the `client` and which side is the `server`
- the maximum frame payload size
- how many concurrent inbound streams each side is willing to serve
- the initial receive window size each side grants to a new virtual stream

## Stream IDs

Each virtual stream has 24-bit unsigned integer ID,
unique relative to all other active streams.

- client-initiated streams use even IDs
- server-initiated streams use odd IDs

IDs may be reused after the old stream is fully closed and retired by both
peers.

## Frame header

Every frame starts with an 8-byte header:

| Bytes | Field | Meaning |
| --- | --- | --- |
| `0..=2` | `stream_id` | big-endian 24-bit stream ID, or first part of a `PING` payload |
| `3` | `flags` | frame type and control bits |
| `4..=7` | `len` | big-endian data length, window delta, or second part of a `PING` payload |

Type bits in `flags`:

| Bit | Name | Meaning |
| --- | --- | --- |
| `0x01` | `PING` | `PING` frame |
| `0x02` | `WINDOW_UPDATE` | window update frame |
| `0x04` | `DATA` | data frame |

Control bits in `flags`:

| Bit | Name | Meaning |
| --- | --- | --- |
| `0x08` | `FIN_READ` | sender stopped reading from this stream |
| `0x10` | `FIN_WRITE` | sender stopped writing to this stream |
| `0x20` | `SYN` | frame starts a new stream, or marks a `PING` request |

Bits `0x40` and `0x80` are reserved and must be zero.

Exactly one of `PING`, `WINDOW_UPDATE`, or `DATA` must be set for a normal
frame. If none of them are set, the header is a `TERM` frame.

## Frame types

### `PING`

`PING` frames carry a 7-byte opaque payload split across `stream_id` and `len`.

- `SYN = 1` means request
- `SYN = 0` means response
- `FIN_READ` and `FIN_WRITE` are forbidden

Each side of the connection can send `PING` requests at will and expect the peer to
echo the exact payload back. `PING` exchanges can be used for keepalive and measuring RTT.

Neither side of the connection can initiate overlapping `PING` exchanges.
This means that a `PING` request can only be sent if:
- this is the first `PING` request from this side, or
- the response for the previous `PING` request has been received

### `WINDOW_UPDATE`

`WINDOW_UPDATE` frames return receive-window credit to the peer.

- `len` is the number of bytes returned to the peer's transmit window
- `len = 0` is valid and is commonly used to carry `SYN` or `FIN_*`
- `SYN`, `FIN_READ`, and `FIN_WRITE` are allowed

### `DATA`

`DATA` frames are followed by `len` payload bytes.

- `len` may be zero
- non-zero `len` must not exceed the negotiated `frame_limit`
- `SYN`, `FIN_READ`, and `FIN_WRITE` are allowed

### `TERM`

`TERM` is encoded as a completely zeroed header:

- `stream_id = 0`
- `flags = 0`
- `len = 0`

It marks the sender's end of the rammux session.

## Stream lifecycle

### Opening a stream

The first frame for a stream ID must carry `SYN`.

That opening frame may be either:

- a `DATA` frame, or
- a `WINDOW_UPDATE` frame

Opening a stream without `SYN`, opening an already active ID, using an ID from
the wrong parity pool, or exceeding the peer's inbound-stream limit is a
protocol violation.

### Independent read and write closure

Each virtual stream is duplex, so the two directions close independently.

`FIN_WRITE` means: "I will not send more data on this stream."
Sending non-empty `DATA` or another `FIN_WRITE` after `FIN_WRITE` is a protocol violation.

`FIN_READ` means: "I am no longer reading data from this stream."
Sending non-empty `WINDOW_UPDATE` or another `FIN_READ` after `FIN_READ` is a protocol violation.

Because zero-length `DATA` and zero-value `WINDOW_UPDATE` frames are valid,
those flags can be sent without accompanying payload or window credit.

## Flow control

Each stream has two independent receive windows, one on each side.
At all times, a receive window size must fit into a 32-bit unsigned integer.

Rules:

1. the sender must not transmit more data than the currently available window
2. each `DATA` payload decrements that available window
3. each `WINDOW_UPDATE(len)` increments it by `len`

This implementation treats the following as protocol violations:

- a `DATA` frame that would underflow the remaining receive window
- a `WINDOW_UPDATE` that would overflow the sender-side window accounting

## Downgrade and transport recovery

Either side may leave rammux by sending `TERM`.

The clean downgrade procedure is:

1. send `TERM`
2. continue reading rammux frames until the peer's `TERM` arrives
4. return the underlying transport

`TERM` is a session-level marker, not a stream-level one. Frames already in
flight can still arrive before the peer's final `TERM`, so a peer that starts
downgrade must keep draining the transport until the handshake completes.
