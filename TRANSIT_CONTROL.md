# Transit control

How rammux decides how much data each stream may have in transit, and what that changes compared with
HTTP/2 and yamux. The wire format is in [PROTOCOL.md](PROTOCOL.md), and the settings are in `RammuxConfig`.

## The problem with windows tuned by the receiver

A multiplexer limits the data in transit with *windows*: the receiver tells the sender how many bytes it is
ready to buffer. A window caps throughput at `window / round trip time`, so a window that fits a fast or a
long path has to be megabytes large, and the mainstream multiplexers tune it at the receiver:

- **HTTP/2** has a window for every stream, and one for the whole connection that all streams draw from.
  Implementations either fix them (hyper: 1 or 2 MiB for a stream by default) or grow them with the measured
  bandwidth (hyper's adaptive window, gRPC).
- **yamux** has a window for every stream. The Rust implementation doubles the window of a busy stream, out of a
  budget that the connection shares, and takes the growth back only when the stream is dropped.

The receiver knows how fast its own application reads. It does not know what the sender has waiting, so
it cannot divide the memory by need. The results, measured below:

1. **Fixed windows cap every stream**, however idle the link is.
2. **The first stream takes the budget, and a latecomer is choked** (yamux). The early stream keeps its
   grown window even when it goes silent.
3. **A slow reader takes the connection window from everybody** (HTTP/2 with large windows). Its unread
   data fills the shared window, and the healthy streams wait for the slow one.

## What rammux does

**The receiver tunes nothing.** It gives every stream a small fixed *base window* and the connection one
shared *pool*, and returns credit as the application reads. That protects its memory, and nothing else.

**The sender divides the pool.** It knows which streams have data waiting and how fast each reader returns
credit, and it decides for every frame:

- a stream uses the pool **in proportion to the rate its reader has proven**, so a reader that slows down or
  stalls stops taking more;
- streams that compete for the pool **take turns**, so a latecomer gets the same share as the stream that came
  first;
- a new or idle stream gets a bounded **advance**, so it does not have to prove itself from zero.

Nothing of this is on the wire. The peer sees only `DATA` that stays within its limits.

## How the sender decides

| Symbol | Meaning |
| --- | --- |
| `B` | Base window of a stream (`remote_recv_window` for the sender). |
| `P` | The peer's pool (`remote_recv_pool`). |
| `unacked` | Bytes sent on a stream and not credited back. |
| `borrowed` | `max(0, unacked - B)`: what a stream uses of the pool. The sum over the streams, `U`, never exceeds `P`. |
| `srtt` | Smoothed round trip time, measured with `PING` and `PONG`. |
| `T_c` | `min(32 KiB, B / 2)`: the step in which the receiver returns credit. |
| `g`, `G` | The advance: what one stream may take, and all streams together (`advance_grant`, `advance_budget`). |

### Within the base window

A stream with less than `B` in flight sends without asking anybody. A stalled stream therefore never stops
another stream from using its own base window.

### The proven rate

Every credit that comes back for a stream feeds an estimate of its reader's rate, a sum that decays with the
time constant `srtt`:

```
R = R * exp(-dt / srtt) + credit / srtt      on every credit
A = 2 * srtt * R                             the allowance
```

A stream may borrow up to its allowance `A`: about twice the bytes that its reader consumed over the last
round trip.

- While the reader keeps up, the window of a stream about doubles every round trip, as in TCP slow start.
- When the reader slows down, the allowance follows it down. The stream keeps about two round trips of its
  read rate in flight, and the rest of the pool stays with the others.
- When the reader stalls, the allowance decays to zero and the stream stops borrowing. What it already
  holds stays in the pool until the reader reads or the stream closes.
- A stream that pauses for more than a couple of round trips starts over, as TCP does after idle.

Until the first `PONG` arrives there is no `srtt`, and streams use their base windows only.

### Turns

When the pool is full, room comes back with credit. If the stream that receives credit were the one
to use the room, it would take back exactly what it returned, and a stream that came later would never get
in. So streams that need the pool wait in a queue:

- only the stream at the front may borrow, even if there is room in the pool;
- a turn is one frame, of up to `frame_limit` bytes. Then the stream goes to the back of the queue;
- a stream that has used up its allowance leaves the queue and waits for its own credit, so it does not hold
  up the others.

Busy streams converge to equal shares of the pool, and with that to equal shares of the bandwidth. Credit and
`FIN_READ` never wait for a turn.

### The advance

With the rules above, a new stream starts with `B` and needs a round trip for every doubling. The advance
lets it start faster:

- a stream that has data to send and less than `T_c` in flight may borrow up to `g` bytes beyond its allowance;
- the advance is for the first flight: it ends with the first credit that comes back, and that credit repays it;
- all streams together hold at most `G` as advance.

Readers that never read can therefore pin at most `G` on top of their base windows, however many they are.
A reader that is slow gets one advance, and after that only what it proves.

## Measured

All numbers come from Linux TCP over netem links, with hyper 1.12 (h2 0.4.20) and yamux 0.13.10. Every
multiplexer has the same memory for the connection, 16 MiB, which is the cap of hyper's adaptive window:
rammux runs with 16 MiB pools and otherwise default settings. Every scenario ran twice, and the tables show
the mean of the runs (the median of ten bursts in the last one).

### A latecomer, and a reader that slows down

A 300 Mbit/s link with a 250 ms round trip. Stream A uploads alone, then B joins, then A's reader slows to
5 Mbit/s, then A stops writing while its reader drains the backlog. TCP alone carries 287 Mbit/s.

| Mbit/s | A alone | A and B | B, while A is read slowly | B, after A stopped writing |
| --- | --- | --- | --- | --- |
| hyper, default windows | 23 | 23 and 23 | 23 | 23 |
| hyper, adaptive window | 285 | 143 and 143 | 5 | 161 |
| yamux | 235 | 265 and 4 | 8 | 8 |
| rammux | 278 | 143 and 143 | 285 | 285 |

With yamux, B stays choked while A is silent, until A is dropped. With hyper's adaptive window, B gets the link back
only as A's reader drains the 16 MiB that A holds.

### A burst on a new stream

A 300 Mbit/s link with a 200 ms round trip, and a connection that already carries a little traffic.
Time until the receiver has read the whole burst:

| ms | 256 KiB | 2 MiB | 8 MiB |
| --- | --- | --- | --- |
| hyper, default windows | 107 | 520 | 2324 |
| hyper, adaptive window | 107 | 159 | 755 |
| yamux | 304 | 734 | 1222 |
| rammux, advance turned off | 504 | 930 | 1364 |
| rammux | 107 | 159 | 418 |
| the link alone | 107 | 156 | 324 |

On a connection that was idle, TCP restarts its own window, and bursts above about 256 KiB are as slow as
TCP makes them, for every multiplexer.

## Costs and limits

- **A latecomer needs a moment.** Next to a stream that holds the pool, a new stream reached 40% of the 300 Mbit/s
  link after 1.25 s (about 3.5 s with the advance turned off). hyper's adaptive window needed 0.25 s, yamux never got there.
- **A reader that stalls keeps what it holds.** Its stream stops borrowing within a few round trips, but the bytes
  that are already in the pool stay there until they are read or the stream closes. When one of two bulk streams
  stalled, it held 10.5 MB of the 16 MiB pool, and the other stream got 205 of 287 Mbit/s
  (8 Mbit/s with yamux, 14 to 136 with hyper's adaptive window).
- **Shares are equal in frames, not in bytes.** A turn moves one frame, and a frame holds at most one write.
  A stream that writes less than `frame_limit` bytes at a time gets a proportionally smaller share next to a
  stream that writes more.
- **More small packets when data flows one way.** Credit is returned every 32 KiB, and every 32 KiB of data
  carries a `PING`. When the other direction has data, these ride along. When it has none, they are about 60
  small packets per MB: 5.5% more packets in total than yamux or hyper, and 0.35 percentage points more bytes.
- **The queue is not limited.** Like the other multiplexers, a connection with enough data fills what it is
  given, here `streams * B + P`, and an interactive stream waits behind the bulk that is already on the path.
  The pool is the bound. `TCP_NOTSENT_LOWAT` can be used as a mitigation on platforms that support it.
- **`TCP_NODELAY` is required.** Without it, credit and `PING`s wait behind Nagle's algorithm and delayed ACKs.

## Choosing the settings

- **The pool** (`local_recv_pool`, and the peer's `remote_recv_pool`). A connection carries at most about
  `(streams * B + P) / round trip time`, and queues up to `(streams * B + P) / bandwidth` on the path. A pool
  of about the bandwidth times the round trip time gives full speed: 300 Mbit/s at 250 ms needs about 9.4 MB.
  A smaller pool means less latency for interactive streams and less memory. Any size works, zero included.
- **The base window** (`local_recv_window`, and the peer's `remote_recv_window`). Every stream gets it, whatever
  the others do, and the memory for it is reserved for every open stream. Keep it small.
- **The advance** (`advance_grant`, `advance_budget`). Fractions of the pool, a quarter each by default.
  A larger grant makes bursts and latecomers faster. The budget is what readers that never read can pin.
  Set both to 0 to turn the advance off.
