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
- **Yamux** has a window for every stream. The Rust implementation doubles the window of a busy stream, out of a
  budget that the connection shares, and takes the growth back only when the stream is dropped.

The receiver knows how fast its own application reads. It does not know what the sender has waiting, so
it cannot divide the memory by need. The results, measured below:

1. **Fixed windows cap every stream**, however idle the link is.
2. **The first stream takes the budget, and a latecomer is choked** (Yamux). The early stream keeps its
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

Nothing of this is on the wire. The receiver sees only `DATA` that stays within its limits. The sender sees only granular `WINDOW` updates.

## How the sender decides

| Symbol | Meaning |
| --- | --- |
| `B` | Base window of a stream (`remote_recv_window` for the sender). |
| `P` | The peer's pool (`remote_recv_pool`). |
| `unacked` | Bytes sent on a stream and not credited back. |
| `borrowed` | `max(0, unacked - B)`: what a stream uses of the pool. The sum over the streams, `U`, never exceeds `P`. |
| `srtt` | Smoothed round trip time, measured with `PING` and `PONG`. |
| `T_c` | `max(1, min(32 KiB, B / 2))`: the step in which the receiver returns credit. |
| `g`, `G` | The advance: what one stream may take without the reader rate proof, and all streams together (`stream_advance_grant`, `conn_advance_grant`). |

### Within the base window

A stream with less than `B` in flight sends without asking anybody. A stalled stream therefore never stops
another stream from using its own base window.

### The proven rate

Every credit that comes back for a stream feeds an estimate of its reader's rate, a sum that decays with the
time constant `1.5 * srtt`:

```
R = R * exp(-dt / (1.5 * srtt)) + credit / (1.5 * srtt)      on every credit
A = 2.5 * srtt * R                                           the allowance
```

A stream may borrow up to its allowance `A`: about two and a half times the bytes that its reader consumes
in a round trip.

- While the reader keeps up, the window of a stream about doubles every round trip, as in TCP slow start.
- When the reader slows down, the allowance follows it down. The stream keeps about two and a half round
  trips of its read rate in flight, and the rest of the pool stays with the others.
- When the reader stalls, the allowance decays to zero and the stream stops borrowing. What it already
  holds stays in the pool until the reader reads, the stream closes, or the writer resets the stream.
- A stream that pauses for more than a few round trips starts over, as TCP usually does after idle.

A stream whose reader keeps up has about one round trip of its rate in flight, so it uses less than half of
its allowance. The rest is slack for credit that comes late. A loss in TCP holds all credit back for about a
round trip, and afterwards the credit of a whole flight comes back in one piece. An allowance without that
slack would stop the stream each time, and streams that are stopped lose their turns and drift apart.

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

All numbers come from Linux TCP over netem links, with hyper 1.12 (h2 0.4.20) and yamux 0.13.10. Every multiplexer has the same memory for the connection, 16 MiB, which is the non-configurable cap of hyper's adaptive window. Every scenario ran twice, and the tables show the mean of the runs (the median of ten bursts in the last one).

### A latecomer, and a reader that slows down

A 300 Mbit/s link with a 250 ms round trip. Stream A uploads alone, then B joins, then A's reader slows to
5 Mbit/s, then A stops writing while its reader drains the backlog. TCP alone carries 287 Mbit/s.

| Mbit/s | A alone | A and B | B, while A is read slowly | B, after A stopped writing |
| --- | --- | --- | --- | --- |
| hyper, default windows | 23 | 23 and 23 | 23 | 23 |
| hyper, adaptive window | 285 | 143 and 143 | 5 | 161 |
| yamux | 235 | 265 and 4 | 8 | 8 |
| rammux | 278 | 143 and 143 | 271 | 285 |

With yamux, B stays choked while A is silent, until A is dropped. With hyper's adaptive window, B gets the link back only as A's reader drains the 16 MiB that A holds.

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

## Costs

- **A latecomer needs a moment.** Next to a stream that holds the pool, a new stream reached 40% of the 300 Mbit/s link after 1.25 s. hyper's adaptive window needed 0.25 s, yamux never got there.
- **Shares are equal in frames, not in bytes.** A turn moves one frame, and a frame holds at most one write. A stream that writes less than `frame_limit` bytes at a time gets a proportionally smaller share next to a stream that writes more.
- **More small packets when data flows one way.** The receiver returns credit for every `T_c` that its reader consumes. Once it has measured the round trip, each side adds a `PING` to its data or credit at most every eighth of it, and the other side answers with a `PONG`. When the other direction has data, they ride along. When it has none, they are packets of their own.

## Choosing the settings

- **The pool** (`local_recv_pool`, and the peer's `remote_recv_pool`). A connection carries at most about `(streams * B + P) / RTT`, and queues up to `(streams * B + P) / bandwidth` on the path. A pool of about one BDP gives full speed: 300 Mbit/s at 250 ms needs about 9.4 MB. A smaller pool means less latency for interactive streams and less memory. Any size works, zero included.
- **The base window** (`local_recv_window`, and the peer's `remote_recv_window`). Every stream gets it, whatever the others do, and the memory for it is reserved for every open stream. Keep it small.
- **The advance** (`stream_advance_grant`, `conn_advance_grant`). Fractions of the pool, a quarter each by default. A larger `stream_advance_grant` makes bursts and latecomers faster. `conn_advance_grant` is what readers that never read can pin. Set both to 0 to turn the advance off.
