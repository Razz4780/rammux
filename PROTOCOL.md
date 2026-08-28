# rammux protocol

This document describes the wire format and session rules implemented in rammux v1.

rammux multiplexes many virtual bidirectional byte streams over one ordered byte
transport. The underlying transport is expected to be a reliable full-duplex
byte stream.

Inspired by [Yamux](https://github.com/hashicorp/yamux), HTTP/2, and QUIC.

## Session preconditions

rammux has no in-band handshake, leaving it customizable to the user.
Therefore before the first rammux frame is sent, the peers must:
* Agree that they both speak rammux.
* Negotiate rammux configuration, including:
    * which side is the client and which side is the server
    * the `DATA` frame payload size limits
    * stream concurrency limits
    * initial sizes of stream receive windows and connection transit windows
    * accepted range of stream priorities

## Stream lifecycle

Every virtual stream has a 16-bit unsigned integer ID, and peers have separate ID pools
they can use for starting a new stream. IDs of closed streams *should* be reused
to prevent ID pool exhaustion.

### Opening a stream

The first frame for a stream ID must carry a `SYN` control flag.
Opening an already active ID, using an ID from the peer's pool,
or exceeding the peer's inbound stream limit is a protocol violation.

### Independent read and write closure

Each virtual stream is duplex, and the two directions close independently.

`FIN_WRITE` control flag is used to say "I will not send more data on this stream."
Sending non-empty `DATA` or another `FIN_WRITE` after `FIN_WRITE` is a protocol violation.

`FIN_READ` control flag is used to say "I am no longer reading data from this stream."
Sending non-empty `WINDOW_UPDATE` or another `FIN_READ` after `FIN_READ` is a protocol violation.

Because zero-length `DATA` and zero-value `WINDOW_UPDATE` frames are valid,
those flags can be sent without accompanying payload or window credit.

### Closing a stream

Each side of the connection considers a stream to be closed only after
both sending and receiving both `FIN_WRITE` and `FIN_READ`.

## Stream-level flow control

Each stream has two independent receive windows, one on each side.
At all times, a receive window size must fit into a 32-bit unsigned integer.

Ground rules of stream-level flow control are:

* The sender must not transmit more data than the currently available window.
* `DATA` frame payload decrements the window by the size of the payload.
* `DATA` frame that underflows the window accounting is a protocol violation.
* `STREAM_WINDOW_UPDATE` frame increments the window by an explicit amount.
* `STREAM_WINDOW_UPDATE` frame that overflows the window accounting is a protocol violation.

## Connection-level flow control

In order to make streams processing more fair, rammux also limits the amount of data
that can be in transit between the two peers. This is because the data in transit
increases latency of a newly opened stream.

This is implemented as two independent receive windows (separate from the stream-level receive windows described above),
one on each side.

Ground rules of connection-level flow control are:

* All frames except `TERM`, `CONN_WINDOW_UPDATE`, `CLEAR_LINK`, and `PING_PONG` decrement the window
  by the total length of the frame. For example, `STREAM_WINDOW_UPDATE` always decrements the window by 8.
* Frame that underflows the window accounting is a protocol violation.
* `CONN_WINDOW_UPDATE` frame increments the window by an explicit amount.
* `CONN_WINDOW_UPDATE` frame that overflows the window accounting is a protocol violation.

## RTT measurement and autotuning

rammux is designed to autotune stream-level and connection-level receive windows at runtime,
based on the nature of the traffic. For this purpose, the protocol implements a multi-step
RTT measurement exchange that allows for finding RTT without worrying about any bloat from in-transit data.

The exchange can be initiated by any peer, and from each peer's perspective
it starts in one of two ways: sending or receiving a `CLEAR_LINK` frame.
The exchange has a strict shape, and is described in the following subsections.

### Receiving `CLEAR_LINK`

The exchange looks as follows:

1. Receive `CLEAR_LINK`
2. Send `CLEAR_LINK`
3. Wait for `PING_PONG` request
4. Send `PING_PONG` response
5. Send `PING_PONG` request
6. Wait for `PING_PONG` response

No other frames — except `TERM` — can be sent after step 2. Protocol resumes as usual after step 6.

Note that a downgrade handshake can be started by either party during the exchange,
and it instantly terminates exchange.

### Sending `CLEAR_LINK`

The exchange looks as follows:

1. Send `CLEAR_LINK`.
2. Wait for `CLEAR_LINK` (other frames can still arrive before it).
3. Participate in two `PING_PONG` exchanges, one initiated by each side.

No other frames — except `TERM` — be sent after step 1. Protocol resumes as usual after step 3.

Note that a downgrade handshake can be started by either party during the exchange,
and it instantly terminates exchange.

## Downgrade and transport recovery

Either side can terminate rammux by sending a `TERM` frame.

The clean downgrade procedure is:

* send `TERM`
* continue reading rammux frames until the peer's `TERM` arrives
* return the underlying transport

`TERM` is a session-level marker, not a stream-level one. Frames already in
flight can still arrive before the peer's final `TERM`, so a peer that starts
downgrade must keep draining the transport until the handshake completes.
All data after `TERM` is no longer a part of the rammux protocol.

## Frame header

In order to progress virtual streams fairly, rammux encodes the data in frames.
Every frame starts with a 64-bit header:

| Bits (on-the-wire order) | Field | Meaning |
| --- | --- | --- |
| `0..=3` | `type` | frame type |
| `4..=7` | `flags` | control flags |
| `8..=15` | `prio` | stream priority level |
| `16..=31` | `stream_id` | 16-bit big-endian unsigned integer, stream ID |
| `32..=63` | `arg` | 32-bit big-endian unsigned integer, meaning depends on the frame type |

Types of frames are as follows:

| Value | Name |
| ----- | ---- |
| `0x0` | `CONN_WINDOW_UPDATE` |
| `0x1` | `DATA` |
| `0x2` | `STREAM_WINDOW_UPDATE` |
| `0x3` | `CLEAR_LINK` |
| `0x4` | `PING_PONG` |
| `0x5` | `TERM` |

Control flags are as follows:

| Bit | Name | Meaning |
| --- | ---- | ------- |
| `0b0001` | `SYN` | start of a stream or a `PING_PONG` exchange |
| `0b0010` | `FIN_WRITE` | write shutdown |
| `0b0100` | `FIN_READ` | read shutdown |
| `0b1000` | `CLIENT_POOL` | whether `stream_id` is from the client's pool |

## Frame types

### `CONN_WINDOW_UPDATE`

This frame is used to manage the connection receive window.

* `arg` is the amount of credit to return to the peer's window.
* `arg=0` is a protocol violation.
* `flags`, `prio`, and `stream_id` must be zeroed.

### `DATA`

This frame is used to transfer a stream data chunk.

* `arg` contains the length of the chunk, and the chunk is transferred right after the header.
* All control flags apply.
* `arg=0` is valid, and can be used for a frame that only carries control flags.
* `prio` is a priority hint for a newly started stream, and must be zeroed when `SYN` is unset.

### `STREAM_WINDOW_UPDATE`

This frame is used to manage the stream receive windows.

* `arg` contains the amount of credit to return to the peer's window for the specific stream.
* All control flags apply.
* `arg=0` is valid, and can be used for a frame that only carries control flags.
* `prio` is a priority hint for a newly started stream, and must be zeroed when `SYN` is unset.

### `CLEAR_LINK`

This frame is used to signal link clearing.
It can only be used in an [RTT measurement exchange](#rtt-measurement-and-autotuning).

* `flags`, `stream_id`, `prio`, and `arg` must be zeroed.

### `PING_PONG`

This frame is used to measure RTT and check peer liveness.
It can only be used in an [RTT measurement exchange](#rtt-measurement-and-autotuning).

* `stream_id`, `prio`, and `arg` must be zeroed.
* Set `SYN` flag indicates a `PING_PONG` request (ping).
  Unset `SYN` flag indicates a `PING_PONG` response (pong).
* Other control flags must be unset.

### `TERM`

This frame is a downgrade handshake.

* `flags`, `stream_id`, `prio`, and `arg` must be zeroed.
