# rammux protocol

This document specifies version 1 of the rammux wire protocol.

rammux multiplexes independent bidirectional byte streams over one reliable, ordered, full-duplex byte
transport, for example a TCP connection. The transport must not delay small writes: on TCP, set `TCP_NODELAY`.

Inspired by [Yamux](https://github.com/hashicorp/yamux).

## Session parameters

There is no handshake. Before the first frame is sent, the peers must agree on:

| Parameter | Meaning | Range |
| --- | --- | --- |
| version | The version of this protocol: 1. | |
| roles | One peer is the *client*, the other is the *server*. | |
| `frame_limit` | The largest `DATA` payload, in bytes. The same for both peers. | 1 to 65 535 |
| stream limit, one per peer | How many streams started by the other peer this peer accepts at a time. | 0 to 32 768 |
| base window `B`, one per peer | How many bytes of each stream this peer buffers for its application. | 1 to 2^32 - 1 |
| pool `P`, one per peer | How many more bytes this peer buffers, for all streams together. | 0 to 2^32 - 1 |

Frames carry no version and no parameters. Peers that do not agree cannot detect it reliably.

## Frames

A session is a sequence of frames in each direction. There is one frame type. All integers are big-endian.

```
[flags: u16][fields, each present if its flag is set][payload]
```

| Bit | Flag | Fields | Meaning |
| --- | --- | --- | --- |
| `0x0001` | `SYN` | `stream_id` | The frame starts the stream. |
| `0x0002` | `FIN_READ` | `stream_id` | The sender of the frame stopped reading the stream. |
| `0x0004` | `FIN_WRITE` | `stream_id` | The sender of the frame stopped writing the stream. |
| `0x0008` | `WINDOW` | `stream_id`, `credit: u32` | The sender of the frame returns `credit` bytes of the stream's window. At least 1. |
| `0x0010` | `DATA` | `stream_id`, `len: u16` | The frame ends with `len` bytes of stream data. `1 <= len <= frame_limit`. |
| `0x0020` | `PING` | `t_ping: u32` | A timestamp to echo. |
| `0x0040` | `PONG` | `t_echo: u32` | The echo of a `PING`. |
| `0x0080` | `TERM` | | The sender of the frame ends the session. |
| `0xFF00` | reserved | | Must be zero. |

The fields follow the flags in this order: `stream_id`, `credit`, `len`, `t_ping`, `t_echo`. The payload follows
the fields. The longest header is 18 bytes.

`stream_id` is a `u16`. It is present once if any of `SYN`, `FIN_READ`, `FIN_WRITE`, `WINDOW` and `DATA` is set,
and all of them then refer to that stream.

A frame has at least one flag. The stream flags can be combined freely, and `PING` and `PONG` can be added
to any frame. `TERM` stands alone.

The parts of a frame take effect in this order: `PONG`, `PING`, `SYN`, `WINDOW`, `DATA`, `FIN_WRITE`, `FIN_READ`.

## Protocol violations

A frame that breaks a rule of this document is a protocol violation. A violation is fatal: the peer that
detects it stops using the session, and the transport cannot be recovered. The violations are:

- reserved bits set, no flags set, or `TERM` combined with another flag;
- `DATA` with `len = 0` or `len > frame_limit`;
- `WINDOW` with `credit = 0`;
- the violations listed under [Streams](#streams) and [Flow control](#flow-control).

## Streams

A stream is a pair of independent byte streams, one in each direction, identified by a 16-bit ID.

### IDs

The client starts streams with even IDs, and the server starts streams with odd IDs. A peer may reuse
an ID once the stream is closed: both peers have sent and received both `FIN_WRITE` and `FIN_READ` for it.
Until then the stream is open, and it counts against the stream limit of the peer that did not start it.

### Opening

The first frame of a stream carries `SYN`, together with whatever else the stream has to send.
A stream that has nothing to send yet does not have to be announced.

These are protocol violations:

- `SYN` with an ID that the receiver of the frame would use for its own streams,
- `SYN` for a stream that is open,
- `SYN` that exceeds the receiver's stream limit,
- a frame without `SYN` for a stream that is not open.

### Closing

The two directions of a stream close independently.

`FIN_WRITE` ends the data that its sender writes to the stream. A peer that receives it sends `FIN_READ`
once its application has consumed the data that arrived, or earlier.

`FIN_READ` tells that its sender discards any more data of the stream. A peer that receives it sends
`FIN_WRITE`, and should not send more `DATA` first.

These are protocol violations:

- `DATA` or a second `FIN_WRITE` after `FIN_WRITE`,
- `WINDOW` or a second `FIN_READ` after `FIN_READ`.

`DATA` that arrives after its receiver sent `FIN_READ` is not a violation. It is discarded.

## Flow control

The receiver of data sets the limits, `B` and `P`, and the sender of data decides how to use them.

For each direction of each stream, both peers count the bytes in flight, `unacked`:

- the sender adds the bytes of `DATA` it sends, and subtracts the `credit` of `WINDOW` it receives;
- the receiver adds the bytes of `DATA` it receives, and subtracts the `credit` of `WINDOW` it sends.

A stream *borrows* `max(0, unacked - B)` bytes from the receiver's pool. The sum over all streams of one
direction is the use of the pool, `U`.

The sender:

1. keeps `U <= P`. Its count is never below the receiver's, so the receiver's `U` then stays within `P` too;
2. chooses freely which stream sends, and how much, within that limit.

The receiver:

1. returns credit for the bytes that its application consumed, and never more than the stream's `unacked`;
2. returns it before the consumed bytes that are not credited yet reach `B`. Otherwise a sender with a full
   window would wait forever. Returning credit in smaller steps lets the sender follow the pace of the reader:
   rammux returns it for every `min(32 KiB, B / 2)` bytes consumed (at least 1).

`FIN_READ` sets `unacked` of the direction to zero, for its sender when it is sent and for its receiver when
it arrives. After sending `FIN_READ`, a peer does not count the `DATA` that still arrives on the stream.

These are protocol violations:

- `DATA` that makes the receiver's `U` exceed `P`,
- `WINDOW` with more `credit` than the stream's `unacked`.

## PING and PONG

Each peer has a private clock: a `u32` that counts microseconds and wraps. `PING`s measure the round trip
time of the session on that clock, and keep an idle session alive.

- A peer may send a `PING` at any time, with any number of them unanswered. `t_ping` is its clock when the
  frame is written.
- A peer that reads a `PING` answers with a `PONG` that has the same value in `t_echo`, as soon as it can.
  It answers only the latest `PING` it has read: a `PING` that a newer one replaced before its `PONG` was
  written gets no answer. A frame carries at most one `PONG`.
- A peer that reads a `PONG` gets one sample: `now - t_echo`, in wrapping arithmetic. Every `PONG` is a sample
  of its own, whenever it arrives.
- A peer does not answer `PING`s after it has sent `TERM`.

A peer may give up on a session when no frame arrives for a time of its choosing. To keep an idle session
alive, it sends `PING`s more often than that: the `PONG`s are the frames that arrive.

## Ending the session

A session ends with an exchange of `TERM`, which leaves the transport ready for other use (a *downgrade*).

1. Either peer may send `TERM` at any time. It is the last frame the peer sends.
2. A peer that has sent `TERM` keeps reading frames, and discards them, until it reads the `TERM` of the other peer.
3. A peer that reads `TERM` before it has sent its own sends `TERM` as its next frame.

The session is over for a peer once it has both sent and received `TERM`. The bytes that follow in either
direction do not belong to rammux. All streams end with the session, and their data that was not
delivered yet is lost.
