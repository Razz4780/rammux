# rammux protocol

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
- the initial transit window size each side grants the other, if any

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
| `0x20` | `SYN` | frame starts a new stream, marks a `PING` request, or marks a spontaneous `CLEAR_LINK` |
| `0x40` | `SESSION` | frame is session-level, not about any one stream |

Bit `0x80` is reserved and must be zero.

With `SESSION` clear, exactly one of `PING`, `WINDOW_UPDATE`, or `DATA` must be
set for a normal frame; if none of them are set, the header is a `TERM` frame.

With `SESSION` set, `stream_id` must be zero and only two combinations are
valid: `SESSION | WINDOW_UPDATE` is a `SESSION_WINDOW_UPDATE`, and
`SESSION | PING` (with or without `SYN`, and with `len = 0`) is a
`CLEAR_LINK`. Anything else is a protocol violation.

## Frame types

### `PING`

`PING` frames carry a 7-byte opaque payload split across `stream_id` and `len`.

- `SYN = 1` means request
- `SYN = 0` means response
- `FIN_READ` and `FIN_WRITE` are forbidden

Each side sends `PING` requests and expects the peer to echo the exact payload
back. `PING` serves two purposes, which the wire format does not distinguish:

- a **plain ping**, sent inline with data, measures the *loaded* round trip -
  the path plus whatever both sides currently have queued;
- a **probe ping**, sent inside a `CLEAR_LINK` exchange, measures the *clean*
  round trip over a drained link.

Neither side may have overlapping `PING` exchanges in flight, so a `PING`
request may only be sent if this is the first from this side, or the response
to the previous one has been received. That, plus the ordering of each
transport direction, is what makes the two purposes decidable: inside a
`CLEAR_LINK` exchange a `PING` is the probe's, and everywhere else it is the
peer's plain one. A `PING` from a peer that provably owes a different frame is
a protocol violation.

An exchange may be abandoned: if a `CLEAR_LINK` takes the link while a plain
ping is outstanding, that ping is forgotten and a response arriving for it
afterwards is ignored rather than treated as a violation.

### `CLEAR_LINK`

`CLEAR_LINK` is `SESSION | PING`, with `stream_id = 0` and `len = 0`. It drains
the link so that the `PING` that follows measures the path alone.

- `SYN = 1` is a spontaneous initiation
- `SYN = 0` is the responder's receipt, sent only in answer to an initiation

A peer that sends `CLEAR_LINK` must stop sending `DATA` and `WINDOW_UPDATE`
frames until the exchange completes. Since each transport direction is ordered,
a receipt can only be produced after everything ahead of the initiation has been
consumed - so receiving one proves both directions are drained. The exchange is:

```text
I: pause data, send CLEAR_LINK+SYN
R: receive CLEAR_LINK+SYN -> pause data, send CLEAR_LINK receipt
I: receive receipt (both directions drained) -> send PING
R: receive PING (I->R is drained) -> send PONG, own PING, resume data
I: receive PONG (clean RTT), receive R's PING -> send PONG, resume data
R: receive PONG (clean RTT)
```

Both sides may initiate at once. Such a collision tie-breaks by role: the client
stays initiator and the server demotes to responder, its own initiation standing
as its pause marker. A colliding initiation is no substitute for the receipt -
it was sent spontaneously, so it proves nothing about the other direction having
drained, and the demoted side still owes a receipt.

Receiving a `DATA` or `WINDOW_UPDATE` frame from a peer that is past its
`CLEAR_LINK` is a protocol violation, as is a receipt without a matching
initiation.

### `SESSION_WINDOW_UPDATE`

`SESSION_WINDOW_UPDATE` is `SESSION | WINDOW_UPDATE`, with `stream_id = 0`. It
returns credit to the peer's transit window (see [Transit window](#transit-window)).

- `len` is the number of bytes returned
- `len = 0` is valid and carries nothing

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

### Transit window

Beyond the per-stream windows, the session may carry one more limit: the
*transit window*, which bounds the total `DATA` payload in flight between the
peers regardless of which streams it belongs to. Per-stream windows govern how
much a receiver is willing to *buffer*; the transit window governs how much may
be *on the link*, which is what keeps a bulk stream from filling the path and
delaying everything sharing it.

Rules:

1. the sender must not transmit more `DATA` payload than its remaining transit
   credit
2. each `DATA` payload decrements that credit
3. each `SESSION_WINDOW_UPDATE(len)` increments it by `len`

Credit is returned as soon as the payload is received and stored in the muxer,
independently of when the application reads it - the bytes have left the link by
then, which is the only thing this window is accounting for.

A transit window of zero disables the mechanism in that direction, and a
`SESSION_WINDOW_UPDATE` sent to a peer that has it disabled is a protocol
violation, as is one that would overflow the sender-side accounting.

## Downgrade and transport recovery

Either side may leave rammux by sending `TERM`.

The clean downgrade procedure is:

1. send `TERM`
2. continue reading rammux frames until the peer's `TERM` arrives
4. return the underlying transport

`TERM` is a session-level marker, not a stream-level one. Frames already in
flight can still arrive before the peer's final `TERM`, so a peer that starts
downgrade must keep draining the transport until the handshake completes.
