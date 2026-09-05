# Transit window: a standalone protocol

Brief for the next agent. Read this, then `rammux/src/global_pool.rs`
(`transit_recv_update`) for the reference implementation of what is being
replaced.

## The task

Build a **new, simple protocol** that does one thing: limit how much data is
in flight, to get **minimum latency at maximum bandwidth**. It exposes an
opaque `AsyncRead + AsyncWrite` over an inner `AsyncRead + AsyncWrite` (a TCP
socket). No logical streams, no multiplexing.

**Do not modify `rammux/`.** rammux's transit window is the reference to beat;
this is a clean-room second implementation of that idea alone, so it can be
iterated on without dragging the multiplexer along.

## The mental model, and it is not the obvious one

Running over TCP, **this window does not control the network queue** - TCP's
congestion controller already does. What it controls is how many bytes the
application has handed to the kernel socket buffer.

That is where the latency comes from. A writer that pushes megabytes into the
socket buffer puts them *ahead* of everything written later; those bytes leave
at link rate whatever the protocol does. The window's job is to keep the
socket buffer shallow while still handing TCP enough to keep its own `cwnd`
fed.

Three consequences worth having in mind before writing any code:

* The target is about **one bandwidth-delay product**, not two. Below it, TCP
  starves and throughput drops. Above it, the excess is standing queue and
  shows up 1:1 in latency: `queue = (W - BDP) / rate`.
* **`SO_SNDBUF` is a second, hidden limiter.** If it is smaller than `W`, it
  binds instead and sets the latency floor. Pin it or at least record it, on
  both ends, or a result will be unattributable.
* `TCP_NODELAY` on both ends. Credit-return frames are small and
  latency-critical; Nagle will hold them.

## What we already established

All of the below is measured, on a userspace link emulator and on real kernel
TCP. Numbers are aggregate goodput and echo round trip.

### 1. The credit re-grant cadence was the whole problem

rammux re-granted credit once **half the window** had been freed. A re-grant
cannot reach the sender before it has drained any window smaller than
`2 x BDP`, so below that size the sender idles with an empty pipe. Everything
else followed: the design had to target `2 x BDP` (a full round trip of
standing queue, the entire latency gap to h2), and the growth rule crawled
because it sized the window from a rate the stalls depressed.

Re-granting once an **absolute 64 KiB** has been freed - `min(64 KiB, W/2)`,
so a small window keeps the old behaviour - fixes it. Fixed windows, 60 ms /
200 Mbit (BDP 1465 KiB); h2 with a fitted fixed window gets 199 Mb / 77 ms:

| window | re-grant at W/2 | re-grant at 64 KiB |
|---|---|---|
| 1.0 x BDP | 104 Mb / 96 ms | **171 Mb / 74 ms** |
| 1.5 x BDP | 140 Mb / 87 ms | **198 Mb / 96 ms** |

From a cold 128 KiB start with autotune, the *unchanged* growth rule went
from 41 to 198 Mb at 60 ms, and 5 to 45 Mb at 200 ms.

**Start here.** Fine-grained credit return is the single highest-value
property. One 8-byte frame per 64 KiB of payload is 0.01% overhead.

### 2. Two growth rules, neither dominant

**`rate-ceiling`** (rammux's existing rule) is *analytic*: while the sender is
consuming the window, `W <- min(2W, c x cleanRTT x rate)`. `cleanRTT x rate`
estimates the BDP, so **`c` is the target as a multiple of BDP**. `c = 2`
today, and that 2 is a fossil of the half-window cadence.

**`rate-plateau`** (built this session) is *empirical*: every `4 x RTT`,
measure the rate the current window sustained; if it beat the previous
window's by >= 1.25x, step up by 1.5x; otherwise hold. First interval is
discarded (it catches the connection's ramp). Grow-only.

Four links, from a 128 KiB start, 64 KiB re-grant, 3 runs each:

| link | BDP | rate-ceiling c=2 | rate-plateau |
|---|---|---|---|
| throttled 10 Mb / 40 ms | 49K | 9.2 Mb / **63 ms** (2.6x) | 9.8 / 115 ms (3.9x) |
| wifi-vpn 100 Mb / 20 ms | 244K | 99.1 / **75 ms** (3.8x) | 99.1 / 81 ms (4.0x) |
| wan 200 Mb / 60 ms | 1465K | 197 / 137 ms (2.3x) | 197 / **90 ms** (1.5x) |
| fat 1 Gb / 60 ms | 7324K | 985 / 105 ms (2.2x) | 961 / **92 ms** (1.5x) |

Ceiling wins low-BDP, plateau wins high-BDP, and the reason is structural:
ceiling computes the target from a formula so it scales down to any BDP;
plateau *searches*, and its 1.5x granularity cannot resolve a 49 KiB BDP from
a 128 KiB start - one step is already 3.9x BDP.

Sweeping `c` at the 64 KiB cadence (totals across the four links):

| | link utilisation | total latency |
|---|---|---|
| c = 2.0 | 97% | 381 ms |
| **c = 1.5** | 93% | **327 ms** |
| c = 1.25 | 72% | 278 ms |

`c = 1.5` gets most of plateau's latency win for one changed constant.
`c = 1.25` is too tight - it buys latency by giving up half the link.

### 3. A fixed window cannot serve the range

The case for autotuning at all. rammux with growth disabled:

| link | BDP | 128K | 256K | 512K | 2048K | autotune |
|---|---|---|---|---|---|---|
| throttled | 49K | 9 Mb / **63 ms** | 10 / 169 | 10 / 380 | 10 / **1668 ms** | 10 / 221 |
| wifi-vpn | 244K | **17 Mb** / 49 | **43** / 47 | 88 / 49 | 99 / 169 | **99** / 81 |
| wan | 1465K | **12 Mb** / 76 | **27** / 75 | **56** / 74 | 198 / 85 | **198** / 90 |
| fat | 7324K | **14 Mb** / 117 | **29** / 78 | **59** / 75 | **236** / 73 | **975** / 92 |

No fixed value is acceptable on more than two rows; the BDP range is 150x.
The autotune column is the only one that reaches the link on all four.

### 4. Where the competition fails

A fixed 256 KiB h2 window (2.25 MiB across 9 streams) fails on both sides:

| | h2 fixed | rammux |
|---|---|---|
| throttled 10 Mb / 40 ms (42x BDP) | 10 Mb / **1417 ms** | 9 / **63 ms** |
| fat 1 Gb / 60 ms (0.28x BDP) | **239 Mb** / 71 ms | **985** / 105 ms |
| wan, **1** bulk stream (0.17x BDP) | **48 Mb** / 71 ms | **197** / 90 ms |

Against *autotuning* comparators rammux wins latency 2.7-13.8x at equal
throughput: on wan, rammux 90 ms against yamux 244 ms and h2-adaptive 367 ms.

## Dead ends - do not retry

* **Delay-gated growth** ("grow while loaded RTT is within X% of clean RTT").
  Direction-blind: a round trip includes *both* sides' queues, so a heavy
  sender reads an inflated loaded RTT that says nothing about the direction
  its own window governs. In an echo workload it froze exactly the side that
  needed to grow. Built, measured, reverted.
* **Stepping the window back** after finding a plateau. Tried with a credit
  "debt" mechanism; stopped connections at a fifth of the pipe. Both rules
  are grow-only for this reason.
* **A credit reserve for small writes.** Rejected as overfitting to a
  benchmark's shape, and not applicable without streams.
* **EWMA on the rate estimate.** Implemented (`rammux/src/rate.rs`,
  time-weighted, `tau = max(4 x RTT, 20 ms)`) and measured: null on a
  steady-state workload, in both directions, on every link. It is kept
  because the raw per-interval rate measured the caller's cadence rather than
  the link, not because it moved a number. Do not expect it to help.
* **A transit-credit "reserve" update trigger** (earlier session): re-grant
  early while a reserve of `rate x RTT` remains. Throughput-neutral,
  consistently worse latency.

## The measurement harness

The proposed design is sound. Notes on making it produce trustworthy numbers.

**Server** - transparent echo, so latency is attributable to the protocol:
read into a 64 KiB buffer; write from it; when the read is pending and there
is unflushed written data, flush.

**Client** - endlessly writes `b'0'`. Each round: after 64 KiB of `b'0'`,
write 512 bytes of `b'1'`; keep writing `b'0'`; latency is from the first
`b'1'` written to the 512th `b'1'` byte received; then immediately start the
next round.

This measures **head-of-line delay under load**, which is exactly what the
window controls - the marker sits behind whatever the protocol allowed in
flight. There is no prioritisation to be had (no streams), so do not add any.

Add these:

* **A raw-TCP baseline.** Same harness, no protocol, straight through the
  socket. That is the floor for latency-without-a-window and the ceiling for
  throughput. Every result is a trade against it. Cheap and essential.
* **A fixed-window sweep** per link (128K / 256K / 512K / 1M / 2M / 4M).
  That curve is the reference the autotune is judged against, and is how
  every finding above was actually found.
* **Goodput over a steady-state window**, skipping the first ~3 s of ramp.
* **Log the window trajectory** with clean RTT and loaded RTT, every ~500 ms.
  Diagnosis is impossible without it; every result above came from that log.

## Measurement pitfalls that cost us real time

* **p99 is bimodal** on these links. A median of 3 runs lands in one cluster
  or the other and looks like a large effect. It needs 6+ runs and the full
  spread, not a median.
* **Anchors.** Run one identical config 2-3 times spread across a session.
  The spread between them is the resolution of every other comparison. On the
  cluster it was 5-20%; a "finding" smaller than that is not one.
* **Release builds only.** A debug build made QUIC look 4x worse than it is.
* **Check `net.ipv4.tcp_congestion_control` before every session.** It
  silently reverted from cubic to bbr mid-session here and the two are not
  comparable.
* **The userspace emulator's loss model is Reno without SACK.** At 0.5% loss
  it collapses every protocol ~40x harder than real CUBIC does. It is
  cross-validated for delay and bufferbloat; do not use it for loss. With
  real netem this is moot, but the same caution applies to any model.
* **Socket buffers can self-inflict loss.** A default-configured quinn
  endpoint reported 0.67% packet loss on *loopback* - the receiver could not
  drain its UDP socket fast enough - which pinned its congestion window at
  236 KiB. Over TCP the analogue is `SO_SNDBUF`/`SO_RCVBUF`; check them.

## Suggested starting point

Framing: `DATA` and `WINDOW_UPDATE` at minimum; a `PING`/`PONG` pair if the
growth rule needs an RTT. rammux's link-clearing probe (pauses data on both
sides, drains, then times a ping) gives a *clean* RTT that a queue cannot
inflate - worth copying if the rule uses `c x RTT x rate`, and worth skipping
if it does not, because the pause is expensive.

Defaults to start from, all measured:

| | value |
|---|---|
| initial window | 128 KiB |
| re-grant threshold | `min(64 KiB, W/2)` |
| growth | `W <- min(2W, 1.5 x cleanRTT x rate)`, grow-only |
| window cap | 4-16 MiB |

The first experiment worth running is the one we could not: **the same sweep
under real `netem` loss**. Every loss result above is from a model known to be
wrong. Whether `c = 1.5` still holds, and whether the plateau rule's search
survives a lossy path at all, are both open.

## Reference points in this repo

| | |
|---|---|
| the rule being replaced | `rammux/src/global_pool.rs`, `transit_recv_update` |
| its config and constants | `rammux/src/config.rs`, `TransitGrowth` |
| rate estimator | `rammux/src/rate.rs` |
| stream window autotune (part 1, not yours) | `rammux/src/stream/inbound.rs` |
| cluster campaign + its findings | `rammux-perf/campaign/HANDOVER.md` |
| protocol wire format | `PROTOCOL.md` |
