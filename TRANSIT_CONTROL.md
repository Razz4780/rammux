# Transit control

How a rammux sender decides how much data to put on the path, and why.
The wire format is in [PROTOCOL.md](PROTOCOL.md). The settings are in `RammuxConfig`.

## Why

Without a limit on data in transit, the only thing that bounds what a sender puts on the path
is the receiver's window. A bulk transfer then queues up to `window / bandwidth` in front of every
interactive exchange: about 335 ms at 100 Mbit/s with a 4 MiB window, 3.4 s at 10 Mbit/s.
TCP, which carries rammux, is a FIFO: a request on stream B waits behind everything stream A put on the path,
however fair the multiplexer is.

The flow control of rammux does two things:

1. **The receiver does not tune its windows.** It grants every stream a fixed *base window* and the connection
   a shared *pool*, and returns credit as the application reads. That protects its memory and nothing else.
2. **The sender decides how much of the pool each stream uses**: in proportion to what the stream's reader
   has proven it reads, and in turns when the pool is contended (the per-stream rules, below).

The connection behaves like the autotuning multiplexers (yamux, HTTP/2 stacks): it fills the
pool. What it adds is fair sharing of the pool between busy streams, and the protection against stalled readers.

## Glossary

| Symbol | Meaning |
| --- | --- |
| `B` | Base window per stream: `local_recv_window` for the receiver, `remote_recv_window` for the sender. |
| `P_tx`, `U_tx` | Sender side: the peer's pool (`remote_recv_pool`), and our use of it. |
| `P_rx`, `U_rx` | Receiver side: our pool (`local_recv_pool`), and the peer's use of it. |
| `unacked_s` | Bytes sent on stream `s` and not credited back. Each side tracks it for both directions of every stream. |
| `borrowed_s` | `max(0, unacked_s - B)`: the stream's use of the pool. `U = sum of borrowed_s`, held in `u64`. |
| `T_c` | Credit threshold, `min(32 KiB, B / 2)`. |
| `R_s`, `A_s` | Rate estimate of the stream (a decaying sum of its credit with the time constant `srtt`), and its allowance `R_s * tau_s`. |
| `tau_s` | `k * srtt`, with `k = 2`: how long the allowance covers. |
| `srtt` | Smoothed round trip time (EWMA, gain 1/8, at least 1 us) of `PING`/`PONG` exchanges. |

## Accounting

Each side tracks `unacked_s` for both directions of every stream:

- sending: add the bytes we send, subtract the credit we receive;
- receiving: add the bytes we receive, subtract the credit when we write the `WINDOW` that carries it.

The receiver's memory is at most `streams * B + P_rx`: every stream holds at most `B` of its own, and
the borrowed parts add up to at most `P_rx`.

**Why the receiver's check cannot trip.** The sender's `unacked_s` is never smaller than the receiver's:
bytes in transit count at the sender only, credit is subtracted at the receiver before it reaches the sender,
and `FIN_READ` zeroes the receiver's counter first. And `max(0, x - B)` never decreases as `x` grows. So a sender that
keeps `U_tx <= P_tx` never makes `U_rx > P_rx`. A frame that does is a protocol violation.

## The receiver

- **Credit** is the bytes the application has read from the stream and not credited yet. `WINDOW(credit)` is sent
  once credit reaches `T_c`, not later (waiting for more would delay the sender) and not for every read
  (control frames are not free).
- **No deadlock:** `T_c <= B / 2 < B` and the sender can always fill `B`. A stream with less than `T_c` uncredited
  bytes therefore always has room to send.
- **After `FIN_READ`** the stream's counter is zero (that returns its share of the pool at the sender too), the
  data that is still in flight is discarded without error, and no credit is sent. Such data still counts in `rx_bytes`.
- Nothing is tuned here: no RTT, no rates, no window growth.

## The sender

### Rate estimate and allowance

Each stream estimates the rate of its credit in bytes per second: a decaying sum with the time constant `srtt`,
about one loop. Its allowance covers `tau_s = 2 * srtt`, two loops, of that rate:

```
R_s.add(bytes)      R_s = R_s * exp(-dt / srtt) + bytes / srtt     (on every credit)
A_s = R_s * tau_s   (R_s at the moment of use)
```

`A_s` is about twice the credit received over the last loop, with older credit fading out: a stream may keep in
flight about twice what its reader consumed over the last loop, and it may hold unread bytes beyond `B` only in
proportion to what its reader has proven it reads. The estimate runs all the time, every credit counts. It sums bytes
instead of timing intervals, so bunching delays credit but never loses it: the mean is exactly `r * srtt` for any arrival
pattern. Don't replace it with a max-filtered rate (BBR style): a max filter turns every bunch into an overestimate.

Two time spans, chosen deliberately:

- **Average over one loop (`srtt`).** After a loop the estimate mostly reflects that loop's credit, so the window of a
  new stream about doubles per loop while its reader keeps up: the growth of yamux and of the adaptive window of HTTP/2.
- **Allow `k = 2` loops (`tau_s`).** The steady allowance is about twice what the stream has in flight.
- **The cost:** a noisier estimate (the ripple of batched credit is `P_c / (2 * srtt)`, which does not matter,
  because the pool binds first), and a stream forgets after a pause of about one loop. Averaging over `tau_s`
  itself would read simpler, but a new stream would grow only about 1.65 times per loop.

**It does not limit delay.** `srtt` includes our own queue, so a longer queue means a longer loop, a larger `srtt`,
and `A_s` keeps growing until the pool stops it. The pool is what bounds the queue.

**What it protects: stalled readers.** When a reader stalls, no credit comes back, `A_s` decays, and the stream stops
borrowing more. What it already holds stays in the pool until its reader reads. This is a documented limit: a
lone bulk stream can hold the whole pool and then stall, leaving other streams with their base windows.
Still better than HTTP/2, where a stalled stream can freeze the shared connection window for everyone.

**Before the first round trip sample** `srtt` is undefined: credit only returns window, `R_s` stays 0, and streams
run on `B` alone. The first `PING` goes out with the first frame.

**A stream that pauses** for more than a couple of round trips ramps up again afterwards, like a new stream. This
is intended. Freezing the estimate during application pauses would only speed up the burst after the pause, and
would send an old allowance into a path that may have changed. TCP restarts its window after idle for the same reason.

### Sending, and the turn queue

**The problem.** Once the pool is full, every busy stream that needs shared room sleeps. The scheduler's round robin
only arbitrates between streams that are awake, so whoever is woken when room frees up decides the split. Credit for
stream X is processed for X, so naively only X is woken. X takes the room it just returned, and the split freezes
forever. Example: X holds the whole 4 MiB pool on a 100 ms, 150 Mbit/s path, so its loop is about 330 ms. Y joins.
Without turns, Y keeps its 64 KiB base window and gets 1.6 Mbit/s, indefinitely.

**The turn queue.** Streams waiting for shared room take turns, one frame per turn (a frame is whatever fits, see below). The queue holds the streams
themselves (identity, so reused stream IDs cannot be confused), and waking the front means waking that stream's
regular poller. A turn carries no byte count: it means "you may use the shared room now", and the stream works out the
amount when its turn comes.

With `base_room = max(0, B - unacked_s)`, `full = min(frame_limit, waiting bytes)`,
`borrow_allow = max(0, A_s - borrowed_s)`, `pool_room = P_tx - U_tx`,
`has_turn` = the queue is empty or the stream is at its front, and `pass_turn()` = leave the queue if queued, and
if `pool_room > 0` wake the new front, the sending direction of a stream does this when polled:

1. **Nothing waiting:** `pass_turn()`, pending.
2. **`full` fits in the base window** (`base_room >= full`): send `full`. No turn needed.
3. **Otherwise the frame needs shared room.** `shared = if has_turn { min(borrow_allow, pool_room) } else { 0 }`
   and `n = min(full, base_room + shared)`.
   - **`n >= 1`:** send `n` bytes, taking `max(0, n - base_room)` from the pool. If it used shared room, the
     stream leaves the front, rejoins at the back if it has more waiting, and wakes the new front if the pool
     has room left.
   - **`n = 0` and `borrow_allow = 0`:** `pass_turn()`, pending. Only the stream's own credit can help, so it waits for
     that, outside of the queue.
   - **`n = 0` otherwise** (no turn, or the pool is empty): join the back of the queue (if not in it), pending. A stream
     already at the front keeps its place.

The front is also woken when credit lowers some `borrowed_s`, or `FIN_READ` releases a share. A stream leaves the
queue when it closes. A stream woken by its own credit keeps its place.

Why:

- **Any frame size is fine.** A frame is whatever fits, up to `frame_limit`: a stream with 8 KiB of room sends
  8 KiB, whether the room comes from the base window, a small pool or a small allowance. Waiting for a whole frame
  would let residue of credit, a small pool or a small base window block a stream that has room. Tiny frames are
  rare, because room frees up in credit chunks of at least `T_c`; a small frame at the tail of a chunk costs one header.
- **Free room never lets a stream skip the queue.** A stream woken by its own credit must not take shared room while
  others are queued ahead of it, even if room is free. Otherwise the split freezes again.
- **No byte-count semaphore.** A stream's allowance keeps decaying while it waits. A semaphore fixes the amount at
  request time, so it could grant a stalled stream more than its allowance, and that stream would hold up the queue.
- **Credit and `FIN_READ` are never held back** by the queue: they go out with the next frame of the connection.

## Dynamics

A stream limited by its allowance delivers `r = (B + A) / L`, where `L` is the loop time, and its allowance moves as
`A` tends towards `k * r * srtt`.

**Growth to the pool.** `A` keeps growing until the pool binds. The queue then settles near
`(N * B + P) / C`, capped by the bottleneck buffer (`N` busy streams, `C` the bottleneck rate). That is the
behavior of yamux, quiche, gRPC and the rammux before this design.

**Startup.** Each loop, a full window `W = B + A` of credit comes back. The estimate averages over about one loop,
so afterwards `R_s` is about `W / srtt` and `A_s` about `k * W`; the next window is about `B + 2 * W`. So the window of a
new stream about doubles per loop (a bit faster at the very start, as `B` comes on top; the simulated link measures
2.3 to 3 times in the first loops). That matches yamux, TCP slow start and the adaptive window of HTTP/2.

**Shares.** Once the pool binds, each stream's allowance has about 2x slack, so the pool and not the allowance
decides each busy stream's share, and the turn queue makes that share equal (in turns, see the known limit 7). Freed room arrives at the link rate
`C` and is handed out in turns, so each of `n` waiting streams gains `C / n`; each stream loses room at its own
delivery rate, proportional to what it holds. The streams settle where each delivers `C / n`; the imbalance shrinks
within about one loop time. A newcomer is limited only by its own allowance ramp, which about doubles per loop: in the example above (330 ms
loops), Y reaches an even split in about 5 loops, roughly 1.7 s.

**Slow and stalled readers** never queue, because their allowance blocks them first. A slow reader keeps its read rate,
and the fast streams share the rest (max-min fair).

## Known limits

1. **New and resumed streams ramp up from `B`** at about twice per round trip: a 0.5-1 MB response on a 100 ms path
   takes 3-4 round trips. hyper with its default 2 MiB windows takes about 1, rust-yamux (256 KiB, doubling) 2-3.
   Responses under `B` are not affected. There are no slow-start shortcuts.
2. **A stalled reader keeps what it already holds** until it reads. If a lone bulk stream holds the whole pool and
   stalls, the other streams are left with their base windows: about 5 Mbit/s at 100 ms with 64 KiB windows.
3. **More control frames than yamux or hyper.** Credit is returned every 32 KiB (against about half a window), and
   every 32 KiB of data also carries a `PING` and gets a `PONG`. At 150 Mbit/s that is roughly 1700 small frames per second.
   Many ride on data frames, and every frame is one write, so the packet count stays close to the data's.
4. **The pool is yours to size.** rammux uses `local_recv_pool` / `remote_recv_pool` as they are, and any
   size works: a pool or a base window smaller than a frame is used with smaller frames.
5. **The order of frames of different streams** on the wire is the round robin of the scheduler. `PONG` and
   `PING` go first, and ride along with the next frame, but the credit of one stream can wait behind data frames of the others.
6. **Set `TCP_NODELAY`.** Without it, small control frames sit behind Nagle and delayed ACKs (measured: about 40 ms).
7. **A turn moves one frame, and a frame holds at most one write.** The turn queue equalizes the number of frames,
   not bytes. A stream that writes less than `frame_limit` at a time sends smaller frames, and gets a share that
   follows the size of its writes. Example: a background stream sending 2500-byte messages competes with a bulk
   stream whose frames are 16 KiB (dsl10): they split the pool about 2500 to 16384, so the background stream gets
   about 13% of it and not half (the simulated link pins a 2 KiB against a 16 KiB writer at 1 to 8). Equal shares between
   busy streams need writes of at least `frame_limit` bytes. Shares by bytes would need turns that carry an amount,
   which has its own costs (see "No byte-count semaphore").
