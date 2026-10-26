# Transit control

How a rammux sender decides how much data to put on the path, and why.
The wire format is in [PROTOCOL.md](PROTOCOL.md). The settings are in `RammuxConfig`.

## Why

Without a limit on data in transit, the only thing that bounds what a sender puts on the path
is the receiver's window. A bulk transfer then queues up to `window / bandwidth` in front of every
interactive exchange: about 335 ms at 100 Mbit/s with a 4 MiB window, 3.4 s at 10 Mbit/s.
TCP, which carries rammux, is a FIFO: a request on stream B waits behind everything stream A put on the path,
however fair the multiplexer is.

The flow control of rammux does three things:

1. **The receiver does not tune its windows.** It grants every stream a fixed *base window* and the connection
   a shared *pool*, and returns credit as the application reads. That protects its memory and nothing else.
2. **The sender decides how much of the pool each stream uses**: in proportion to what the stream's reader
   has proven it reads, and in turns when the pool is contended (the per-stream rules, below).
3. **`CopaLike` only: a connection-wide window** on the bytes on the forward path, which keeps the queue
   of the connection near a fixed size (see [CopaLike](#copalike-a-copa-style-connection-window)).
   `RammuxConfig::transit_control` selects the mode. It is a local choice: it only decides what our sender may put
   on the path, and the receiver behaves the same in both modes, so the peers need not agree.

In `Off` mode (no transit control: only the per-stream rules), the connection behaves like the autotuning multiplexers
(yamux, HTTP/2 stacks): it fills the pool. What it adds is fair sharing of the pool between busy streams, and the
protection against stalled readers.

## Glossary

| Symbol | Meaning |
| --- | --- |
| `B` | Base window per stream: `local_recv_window` for the receiver, `remote_recv_window` for the sender. |
| `P_tx`, `U_tx` | Sender side: the peer's pool (`remote_recv_pool`), and our use of it. |
| `P_rx`, `U_rx` | Receiver side: our pool (`local_recv_pool`), and the peer's use of it. |
| `unacked_s` | Bytes sent on stream `s` and not credited back. Each side tracks it for both directions of every stream. |
| `borrowed_s` | `max(0, unacked_s - B)`: the stream's use of the pool. `U = sum of borrowed_s`, held in `u64`. |
| `T_c` | Credit threshold, `max(1, min(32 KiB, B / 2))`. |
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
- **No deadlock:** for `B >= 2`, `T_c <= B / 2 < B` and the sender can always fill `B`, so a stream with less than `T_c`
  uncredited bytes always has room to send. For `B = 1`, `T_c = 1 = B`: the one byte is credited as soon as it is read,
  so the sender never waits for credit that is not coming.
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
- **The cost:** a noisier estimate (the ripple of batched credit is about `batch / (2 * srtt)`, where a batch is the credit returned at once, at least `T_c`; it does not matter,
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
  The connection window of the CopaLike mode is the exception, see below: it never splits a frame.
- **Free room never lets a stream skip the queue.** A stream woken by its own credit must not take shared room while
  others are queued ahead of it, even if room is free. Otherwise the split freezes again.
- **With the connection window, a frame within the stream's base window skips the turns** (fq_codel's sparse flows).
  The turns share the connection window between streams that borrow; a stream within its base window sends at most a
  base window per round trip, so letting it go first costs the borrowers little and keeps small requests and replies from
  waiting behind their frames. If the window has no room, it waits at the front of the queue. On emulated 100 ms paths
  with bulk both ways this removed about 1 ms at the median and up to 25 ms at the 99th percentile that small writes
  waited behind bulk turns.
- **No byte-count semaphore.** A stream's allowance keeps decaying while it waits. A semaphore fixes the amount at
  request time, so it could grant a stalled stream more than its allowance, and that stream would hold up the queue.
- **Credit and `FIN_READ` are never held back** by the queue: they go out with the next frame of the connection.

## Dynamics

A stream limited by its allowance delivers `r = (B + A) / L`, where `L` is the loop time, and its allowance moves as
`A` tends towards `k * r * srtt`.

**Growth to the pool.** `A` keeps growing until the pool binds. The queue then settles near
`(N * B + P) / C`, capped by the bottleneck buffer (`N` busy streams, `C` the bottleneck rate). That is the
behavior of yamux, quiche, gRPC and of `Off` mode.

**Startup.** Each loop, a full window `W = B + A` of credit comes back. The estimate averages over about one loop,
so afterwards `R_s` is about `W / srtt` and `A_s` about `k * W`; the next window is about `B + 2 * W`. So the window of a
new stream about doubles per loop (a bit faster at the very start, as `B` comes on top; the simulated link measures
2.3 to 3 times in the first loops). That matches yamux, TCP slow start and the adaptive window of HTTP/2.

**Shares.** Once the pool binds, each stream's allowance has about 2x slack, so the pool and not the allowance
decides each busy stream's share, and the turn queue makes that share equal (in turns, see limit 7 of "Known limits (per-stream rules)"). Freed room arrives at the link rate
`C` and is handed out in turns, so each of `n` waiting streams gains `C / n`; each stream loses room at its own
delivery rate, proportional to what it holds. The streams settle where each delivers `C / n`; the imbalance shrinks
within about one loop time. A newcomer is limited only by its own allowance ramp, which about doubles per loop: in the example above (330 ms
loops), Y reaches an even split in about 5 loops, roughly 1.7 s.

**Slow and stalled readers** never queue, because their allowance blocks them first. A slow reader keeps its read rate,
and the fast streams share the rest (max-min fair).

## Known limits (per-stream rules)

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
   stream whose frames are 16 KiB: they split the pool about 2500 to 16384, so the background stream gets
   about 13% of it and not half (the simulated link pins a 2 KiB against a 16 KiB writer at 1 to 8). Equal shares between
   busy streams need writes of at least `frame_limit` bytes. Shares by bytes would need turns that carry an amount,
   which has its own costs (see "No byte-count semaphore").

## CopaLike: a Copa-style connection window

The per-stream rules decide who gets the pool. They do not keep the queue on the path short: `srtt` includes our
own queue, so the allowances grow until the pool binds. `CopaLike` adds one window `W` for the whole connection,
which does.

The idea is Copa's (Arun and Balakrishnan, 2018), in its default mode: a connection aims to keep a fixed number of
bytes, `alpha` (`queue_target_bytes`, 64 KiB by default), in the bottleneck queue, and moves its window up or down
by `alpha` per round trip, spread over the `PONG`s of the round trip (Copa's per-ACK update). Differences from Copa:

- the target is `alpha` bytes and not `1/delta` packets;
- the signal is the **one-way** forward queue, which the reverse direction does not disturb;
- in-flight counts only bytes on the **forward** path (`fwd_in_flight`), which keeps bulk in both directions stable;
- the excess over `alpha` is shed in about a round trip (the excess cut), instead of Copa's velocity;
- Copa's minimum of the last 10 s is replaced by a checked **re-baseline**.

What we give up: `CopaLike` does not compete with buffer-filling traffic such as a CUBIC download. Against it, the
connection yields most of the link (the queue is the competitor's).

**Why a window, why connection-wide, and why delivered bytes.** Every stream's bytes wait in the same TCP queue, so
the connection limits the delay once. The window counts delivery to the peer's rammux (`rx_bytes` in `PONG`s) and not
credit, because credit waits for the application to read, and one stalled reader would then block everyone: 300 KB
unread in a stream that nobody reads would sit inside `W` forever. This is the split TCP makes: the receive window
covers the reader, the congestion window the network.

**Why it is fair.** At the fixed point every connection holds `alpha` bytes queued: `rate * q_fwd = alpha`.
Connections that share a bottleneck see the same `q_fwd`, so they get the same rate, whatever their base round trip.
Copa, Vegas and FAST get their fairness this way. A purely relative rule cannot be fair, because scaling one flow's
window keeps every equation balanced.

### Measurements

All are per connection and updated on every `PONG` (the clocks of the two sides have unknown offsets, which cancel):

| Name | Definition |
| --- | --- |
| `owd_out` | `t_read - t_echo`: our `PING`'s trip to the peer, plus the offset of the clocks. |
| `owd_back` | `now - t_read`: the `PONG`'s trip back, including any wait behind the peer's data, minus the offset. |
| standing | The minimum of a quantity over the last `srtt / 2`, the newest sample included. It filters out jitter and bunching but keeps a queue that persists. |
| `owd_out_min`, `owd_back_min` | The base delays. A lower sample lowers them at once; they rise only in the re-baseline test. |
| `q_fwd`, `q_back` | `max(0, standing - min)` of `owd_out` and `owd_back`: the queue on our forward path, and how long the reports of the peer wait on their way back. |
| `R` | The delivery rate: a rate estimate with the time constant `srtt`, fed with `rx_bytes` deltas (the first `PONG` only sets the baseline). |
| `own` | `R * q_fwd`: the bytes we hold in the queue (Little's law). |
| `in_transit` | `tx_bytes - peer_rx`, with wrapping arithmetic. |
| `fwd_in_flight` | `in_transit - R * q_back`: in transit, minus the bytes that are delivered but whose report is late. |

**Why `fwd_in_flight`.** With bulk in both directions, `PONG`s queue behind the peer's data. When the peer's queue
drains, a backlog of reports arrives at once, `in_transit` collapses, and a window on `in_transit` would release a burst
of megabytes: the ACK compression of two-way traffic. Subtracting what is delivered but not reported yet keeps both
directions busy and short. (In simulation, bulk both ways reaches about 80% of the link without the subtraction and
about 93% with it.) The subtraction does not grow with the time since the last `PONG`: a stall of the forward path has
no reports, and must not open the window. Bursts are the pacer's business.

### The window

- **Start and floor.** `W` starts at 4 frames and never drops below 4 frames: one floor everywhere (the excess cut, the
  steps, the stop of the growth), so that the `PING` spacing of `W / 4` bytes always leaves room for a frame.
- **Enforcement.** Every DATA byte counts against the window: a frame is sent only if `W - fwd_in_flight` has room for
  all of it. A frame that borrows from the pool also needs the turn (`conn_room = if has_turn { W - fwd_in_flight } else
  { 0 }`). A frame within the stream's base window skips the turns: it needs only room in the window, and if there is
  none, the stream waits at the front of the queue. A stream that the window or the turn keeps from sending joins the
  queue, and the front is woken when a `PONG` frees room. Credit, `PONG`s and `PING`s are never held back by the window.
- **The window never splits a frame** (silly window avoidance, RFC 813 and RFC 1122 4.2.3.4). A frame the stream
  could send otherwise goes whole, or waits until the window has room for all of it. Room in the window opens with
  every `PONG`, a few bytes at a time, and a blocked frame asks for a `PING`, whose `PONG` opens a few more: a sender
  that fills every opening ends up sending a frame and a `PING` per `PONG`. On a 150 Mbit/s path with 100 ms of delay
  that was hundreds of thousands of frames of 20 bytes per second each way, whose headers, segments and CPU made the
  queue that the window then measured (about half of the link was data). Waiting cannot block for good: a frame is at
  most `frame_limit`, which is the floor of `W`, so a drained window always has room for it.
- **Pacing** (Copa's rule): DATA leaves at most at `2 * W / srtt`. A burst is at most two frames plus 2 ms of that rate
  (`2 * frame_limit + pace_rate * max(2 ms, timer_lateness)`), where `timer_lateness` is the smoothed lateness of the
  pacer's own timer: on a platform whose timer wakes the sender every 15.6 ms (Windows), 2 ms of burst would cap the
  rate at a fraction of the link, so the burst follows the timer. Without pacing, data leaves when delivery reports
  arrive. With bulk both ways the reports wait behind the peer's data and arrive in clumps (the ACK compression of
  two-way traffic), so the window opens in clumps and goes out in bursts at the speed of the local link. The
  bottleneck queues each burst and then idles until the next one. On a 150 Mbit/s path with 100 ms of delay and bulk
  both ways, an unpaced sender was idle in half of the 20 ms slots, and its download ran about 10% lower with 25-70 ms
  of queue; paced, it is idle in 1% of them. A userspace sender cannot see the queues that bunch its reports, so it
  keeps its own clock. A frame that the pacer holds back wakes the sender on a timer, and only the window (not the
  pacer) counts as limiting for the growth of `W`.
- **One law, two states `(W, g)`.** `W` is the window; `g` is the growth, in bytes per round trip, while the queue is
  below the target. There is no startup mode: startup is `g` growing from the first round trip. Each `PONG` that
  delivers `delivered` bytes has the share `s = min(delivered / W, 1)` of a round trip:

  ```text
  stop = max(own, own8) >= alpha          own8: the same test on the newest 8 raw samples
  if stop and g > alpha:  W = min(W, fwd_in_flight), g = alpha
  if own <  alpha and limited:  W += g * s
  if own >= alpha:              W -= (alpha + max(0, own - 2 alpha)) * s        (the excess cut)
  ```

  Once per round trip (when a `PONG` arrives for a `PING` sent after the previous update), `g` doubles, but never beyond
  `W`, if all of these hold, else `g = alpha`: every `PONG` of the round trip had `own < alpha / 2`; the window is
  `limited`; and the rate rose by at least `alpha / (2 srtt)`.
  - **`limited`** has one meaning everywhere: the window kept a frame from being sent within the last `srtt`.
  - **The stop.** A geometric `g` overshoots by what the delay of the measurement hides, about a doubling. It ends on
    the first `PONG` that shows `alpha` queued, by the standing minimum of `srtt / 2` (`own`) or by the smallest of the
    newest 8 raw `owd_out` samples above the base delay (`own8`, HyStart's 8 samples, against jitter), whichever is
    first, and the window keeps only what is on the path (the unsent part of a doubled window would only queue). The
    update alone sees the queue of a doubling only a round trip later, when the window has doubled again, which is 1 to
    3 bandwidth-delay products of excess. The same stop ends the first growth and the recovery after room appeared,
    because it is the same physics.
  - **The rise.** The threshold does not depend on `g`: the rate lags the window by 2-3 round trips, so the rise it
    shows is the step of two updates back, and a threshold of `g / 2` would fail at the third doubling by construction
    (a 1-2-4 sawtooth). That lag is also the test's safety: if the TCP below, not the window, limits the rate, the rate
    does not rise and `g` falls back to `alpha` within a round trip. While the rate has less than two `srtt` of
    deliveries (its first rounds, from zero), its rise says nothing and the test is passed: a property of the
    estimate, not a mode.
  - **The excess cut.** The decrease sheds the excess over `alpha` in about one round trip, spread over the `PONG`s,
    so that it follows the mean of the round trip's measurements and not the `own` of one spike. Its gain of 1 is
    load-bearing: the `own` it reads is a round trip old, so it over-cuts a falling queue by about `1 + q / d` (`q` the
    queue, `d` the delay of the measurement), and that over-cut is what lets the queue touch nearly empty within each
    re-baseline history (see the drain invariant below). When the capacity drops, steps of `alpha` per round trip could
    not shed the excess while the round trip itself is seconds long; the excess cut does, and a drop from 150 to 10
    Mbit/s is back under 150 ms of queue in about 3 to 4 s.
  - **The steps are spread over the `PONG`s** (Copa's per-ACK update): each `PONG` moves `W` by `alpha * delivered / W`,
    in the direction its own `own` says. With one step per round trip, connections on one bottleneck would see the same
    queue swing by `2 alpha / C` per round trip, more than the band `alpha / R1 .. alpha / R2` where only the faster one
    steps down. Both would step the same way at nearly every update, and the split would freeze wherever the phase of the
    updates in the swing put it (in simulation 0.39-0.62 for two equal connections; spread, 0.46-0.52). The cost of
    spreading: the queue averages `alpha`, and when `alpha / C` is as large as the loop (52 ms at 10 Mbit/s), the swing of
    the loop is about as large as the queue, so the queue empties 4-7% of the time.
- **No idle decay.** An update where `W` did not limit sending neither grows nor lowers `W`, and the growth does not
  start again: after an idle period `W` is as it was. Cutting `W` to what was used (TCP's congestion window validation)
  and restarting startup is not needed: the pacer bounds the burst of a restart, and the decrease (the excess cut and
  the steps, which never wait for `limited`) brings `W` down if the path got slower.

`PING`s: the window needs a fresh sample every round trip. On top of the usual cadence, a `PING` goes out after
`min(ping_bytes, W / 4)` bytes of DATA, and at once when the window blocks a frame and DATA was sent since the last
`PING`.

### Couplings

Four places where two rules depend on each other. Each is written at its rule in `delay.rs`; change one side and
check the other.

- **The gain of 1 of the excess cut and the re-baseline drain.** The `own` that the cut reads is a round trip old, so the
  cut over-cuts a falling queue, and that over-cut is what lets the queue touch nearly empty within each re-baseline
  history. A smaller gain leaves the queue standing and the base delay creeps up.
- **The lag of the rate and the safety of `g`.** The rate lags the window by 2-3 round trips; if the TCP below does not
  answer a doubling with a rate, the rise test fails and `g` returns to `alpha` within a round trip. A rate with no lag
  would let a doubling overshoot by megabytes against a send buffer.
- **`ping_bytes` and the sample windows.** The 8 newest samples span `8 * min(ping_bytes, W / 4) / R`, so the `PING`
  spacing also sets the speed and jitter tolerance of the 8-sample stop and the number of samples in the standing
  minimum.
- **The pacer's `srtt` and the window.** `srtt` includes `q_back`; the window excludes `R * q_back`. Once
  `q_back > RTT_base + q_fwd` the pace `2 W / srtt` falls below `R`, and `W` stops growing while the pacer binds. The
  pacing gain of 2 is the margin that hides this in steady state; a reverse queue beyond a round trip is what it does
  not cover.

**The drain invariant.** At low bandwidth-delay product (`alpha` larger than the BDP, e.g. 10 Mbit/s with a 40 ms round
trip), the queue empties only because the excess cut over-cuts, and that works because `R * q_fwd` overstates the bytes we
really hold. Capping `own` by `fwd_in_flight` is physically true but loose by about a BDP, and it removed that
overstatement: on a 10 Mbit/s link the share of 1 s windows draining to 2 ms or less fell from 0.65 to about 0,
re-baselines rose from 2 to 12 per minute, and a 15 ms standing queue appeared. So any change to `own` or to the excess
cut must be checked against the drained share (0.65) and the re-baseline rate (about 2 per minute).

### Keeping the base delays honest: the re-baseline test

The base delay can rise: a route change, a delay swing, clock drift, a queue of someone else that we cannot drain. Only our
own response tells these apart from our queue. On every `PONG`, for each direction (the same test for both), the raw
samples of the last 5 `srtt` are compared with the *current* base delay (a queue computed against an older base would make
the test fire again on every `PONG`): if no sample had a nearly empty queue, the base delay is raised to the smallest raw
sample. There is no condition on `W`, so a steady phantom delay never counts as nearly empty, which covers idle and
application-limited connections. The test needs a history of 5 `srtt` since the first sample or since the last
re-baseline (otherwise it takes the queue of the startup for the path, and repeats as soon as the sample it was based on
leaves the window).

"Nearly empty": some sample of the window had a queue of at most a quarter of the largest standing queue of the window
(the standing value, the minimum of `srtt / 2`, keeps spikes of jitter out of the peak). Copa's own test uses 10% of the
largest raw sample; it assumes that the queue empties, and two connections of ours with different round trips do not empty
their joint queue together (below).

The test is sound only if our own queue empties within its 5 round trips. With one step of `alpha` per round trip it does
not (the queue cycles between `alpha / 2` and `2 alpha`). With the steps spread over the `PONG`s the queue swings around
`alpha` and empties now and then, and the test does not ratchet: in simulation, one or two connections hold the same ping
round trip from the start to the end of minutes of bulk.

**Alternatives that fail.**

- Also counting a queue of at most `2 alpha / R_max` (`R_max`: the largest rate of the last 10 s) as nearly empty, i.e. one
  that our own target explains. It seals a phantom delay in: at the fixed point the rate is about `alpha / phantom`, and
  that rate is what makes `2 alpha / R_max` large enough to explain the phantom. Per-packet jitter is such a phantom: a
  path that keeps packet order makes each packet wait for the slowest one before it, so under load the path is slower
  than the fastest sample, which sets the base delay, by most of the jitter. On an emulated path with 20 ms of such
  jitter, this variant gave bulk download 36 Mbit/s against 91 Mbit/s with the quarter-of-peak test, and two connections
  split 0.24 / 0.76 instead of 0.51 / 0.49.
- A drain of the queue on top of the steps cost 3-15% of throughput where it was tried.
- Copa's 10%, with two connections whose round trips differ (one 50 or 100 ms farther each way, through the same
  bottleneck): their joint queue does not empty within 5 round trips of each, they take part of each other's queue for the
  path, and the ping round trip climbed from 65 to 140-185 ms in 120 s on a 100 ms path (from 113 to 160-175 ms on a
  path with large delay swings). The queue did empty, just not often enough. With a quarter of the standing peak it stays
  at 67 and 112-118 ms, with no cost found elsewhere (jitter, delay swings, 10 and 100 Mbit/s, bulk down, both ways, two
  connections). The connection with the longer round trip gets 0.56-0.62 of the two (multiplexers without a window give
  it 0.30-0.41).

The back direction uses the same test. Its base delay can take in part of the bottom of the peer's queue of `alpha`;
that makes `q_back` smaller and `fwd_in_flight` larger, so it errs on the side of sending less.

### Expected behavior

- **Utilization** 93% to 100% in steady state; the queue is about `alpha / C` per connection of ours (3.5 ms at
  150 Mbit/s, 52 ms at 10 Mbit/s), with the additions of the re-baseline test.
- **Bulk both ways** is decoupled through `fwd_in_flight`.
- **Fairness:** two connections split 50/50, also when one starts 5 s later and when one has 100 ms more round trip.
  Connections that re-baseline at different times make the split noisy (four connections at 10 Mbit/s: 19% to 36%).
- **The queue grows with the number of our connections:** `N alpha / C`. Four connections at 10 Mbit/s with 64 KiB each
  make about a second in the simulation.
- **A CUBIC-like competitor** gets most of the link (we keep about 12% in the simulation); the queue is theirs. No
  starvation.
- **Interactive frames:** a frame within the stream's base window skips the turns, so a small request does not wait
  behind the frames of the bulk streams for a turn; it waits only for room in the window, at the front of the queue.
  Frames that borrow from the pool wait their turn, one frame per turn each.
- **After an idle period** the window is as it was, and does not grow while the application does not limit it. The
  restart goes out at the pacing rate (bursts of at most two frames plus 2 ms), and the first `PONG`s measure the queue it
  makes. A stale window on a changed path is corrected by the decrease, not by a restart.
- The pool (`local_recv_pool`) is only a memory limit in this mode. If it is smaller than what the path needs in flight,
  it limits the throughput first (about 5.6 MB at 150 Mbit/s with a 300 ms loop).

### Known limits (CopaLike)

Compared with raw TCP under the same impairment, on emulated links:

- **Stall or outage recovery.** After a 1 s hold (rate near 0), time to 90% of the rate is about 2.5 s against raw's 1.1 s,
  and the 10 s ratio is 0.82. Cause: `PING`s sent before the stall return after it, the standing minimum keeps that stale
  delay, `own` shows a phantom queue, and the excess cut takes `W` to the floor. The queue stays bounded (about 17-25 ms at
  the 95th percentile). Untried: drop delay samples from `PING`s sent before a gap in deliveries.
- **Wi-Fi aggregation.** With 64 KiB aggregates: 0.88 of raw at `alpha` = 64 KiB, 0.91 at 128 KiB, because `alpha` is about
  one aggregate. Raise `queue_target_bytes` on aggregating links.
- **One-way loss starves the reverse feedback.** With bulk both ways and loss on one direction, the clean direction gets
  0.41-0.75 of raw: its reports come back behind the lossy direction's retransmissions. This is structural (a single byte
  stream); `Off` is worse.
- **Low-BDP links:** on a 10 Mbit/s link, bulk both ways (upload) is about 3% and a CUBIC upload about 5% below what a
  window that grows geometrically only in startup reaches, because `g` may double up to `W` at small `W`.
- **A rise of the base delay** is cut as if it were our queue until the re-baseline accepts it (16-21% of 10 s throughput
  for 0.6-0.7 s).
- **A clock-rate offset of the peer** shows as a phantom delay that grows with time, and is removed by the re-baseline test.
- **CUBIC share** is small by design.
- **The pacer's timer lateness** is sampled as the time from the timer's deadline to the moment the connection task
  noticed it, which also includes delays that are not the timer's (a transport that was not writable, a batch of inbound
  frames, an application that did not poll). A long delay of that kind raises the burst of the pacer for many wake-ups: the lateness decays by a factor of 7/8 per pacer wake-up (about 16 wake-ups to an eighth), and wake-ups come only while the pacer is armed.
- **Windows:** slow start after idle, and the timer-driven pacer on a 15.6 ms timer, are unverified.
- **Passed:** uniform loss (0.1%, 1%) and burst loss (TCP is the limit; no over-cut), and GRO/TSO/GSO on (no effect).
