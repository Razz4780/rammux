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

* **Delay-gated growth off the *round-trip* delay** ("grow while loaded RTT
  is within X% of clean RTT"). Direction-blind: a round trip includes *both*
  sides' queues, so a heavy sender reads an inflated loaded RTT that says
  nothing about the direction its own window governs. In an echo workload it
  froze exactly the side that needed to grow. Built, measured, reverted.
  **This does not condemn delay-based control** - only round-trip delay as
  the signal. One-way delay fixes the direction problem outright; see
  "Estimating bandwidth" below.
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

## Estimating bandwidth, and the loop that makes it hard

The measured rate is limited by the window, and the window is set from the
measured rate. That circularity is the reason the ceiling rule crawls, and
the first thing to fix.

**The rate error is one-sided.** While `W < BDP` the connection is
window-limited and `rate = W / RTT` - an *under*estimate. While `W > BDP` it
is link-limited and the sample is correct. Nothing can ever *over*estimate
the bottleneck. So the right statistic is the **maximum**, not the mean, and
any averaging filter - the EWMA included - mixes underestimates into the
answer and drags it below truth. `rate_ema` is simply the wrong statistic for
this quantity, whatever its parameters.

Four ways forward, roughly in order of cost:

**1. Max-filter, as BBR does.** `BtlBw = max(delivery_rate)` over a sliding
window of about 10 round trips; `RTprop = min(RTT)` over a longer one;
`BDP = BtlBw x RTprop`. The max selects the samples that were not
window-limited; the sliding window still lets a bandwidth that has genuinely
gone away be forgotten. Flag **app-limited** samples - taken when the sender
had credit spare and did not fill it - and let them raise the max but never
lower it, or one idle moment poisons the estimate. Cheapest real fix
available.

**2. Read the drain you already pay for.** A link-clearing probe *is* a
bandwidth measurement. At the pause, `B` bytes are outstanding, and
`CLEAR_LINK` sits behind them in the ordered stream, so the peer cannot
acknowledge it until all `B` have landed:

```
receipt_time = B / bandwidth + RTT_prop
bandwidth    = B / (receipt_time - RTT_prop)
```

`RTT_prop` comes from the clean ping immediately after. No circularity: `B`
is set by the window, but the *ratio* is the bottleneck rate whatever `B` is,
provided it is more than a packet or two. Infrequent but clean - use it to
seed and correct the max filter rather than as the only source.

**3. Skip bandwidth entirely - control on queue.** Vegas computes queued
bytes from delay alone, `queued ~ W x (loadedRTT - baseRTT) / loadedRTT`, and
targets a small constant queue. Both RTTs are already available. It inherits
the direction-blindness above, though.

**4. One-way delay, which fixes that** - LEDBAT, RFC 6817. Sender timestamps
each frame; receiver computes `owd = arrival - timestamp`, which carries an
unknown constant clock offset; receiver tracks `base = min(owd)` over a long
window; `queuing_delay = owd - base` and **the offset cancels**, no clock
sync needed. That is a per-direction queue signal, and it measures the exact
quantity the benchmark reports. Watch for clock *drift* corrupting `base`
over a long connection - LEDBAT rolls the base over minutes.

**Not available here: packet-pair and packet-train** (pathchar, TOPP). They
read the bottleneck from inter-arrival spacing, and over TCP this protocol
sees a byte stream with no visibility into or control over segment
boundaries. Rule it out early.

**The plateau rule was BBR's ProbeBW, slowly.** "Raise the window, see
whether the rate actually rises" is the only way to break the loop by
experiment rather than by statistics, and it is what BBR does by cycling
`pacing_gain` through `[1.25, 0.75, 1, 1, 1, 1, 1, 1]` over eight round
trips. Two differences matter: BBR **paces** rather than opening a window, so
the excess does not arrive as a burst, and it **drains on the next round
trip**, so a failed probe costs one round trip of queue rather than a
permanent step. That is why 25% probes are safe where our 50% window steps
were not.

## Suggested starting point

Framing: `DATA` and `WINDOW_UPDATE` at minimum. Add a `PING`/`PONG` pair if
the rule needs a round trip, and a per-frame timestamp if it uses one-way
delay. rammux's link-clearing probe gives a *clean* RTT that no queue can
inflate, and doubles as the bandwidth meter in (2) above - but it pauses both
sides, so it earns its place only if the rule actually uses it.

Measure at the **receiver**. It knows exactly when bytes arrived, with no
ACK-clock inference, and it is already sending `WINDOW_UPDATE` frames that an
estimate can ride along on.

Defaults to start from. The first two are measured and solid; the growth rule
is the open question, and the form below is the *old* one with its constant
corrected - it is a fallback, not a recommendation, because a mean rate is
the thing this section says is broken:

| | value |
|---|---|
| initial window | 128 KiB |
| re-grant threshold | `min(64 KiB, W/2)` |
| window cap | 4-16 MiB |
| growth, fallback | `W <- min(2W, 1.5 x cleanRTT x max-filtered rate)`, grow-only |

Two branches worth trying before settling: **BBR-shaped** - max-filtered
`BtlBw`, `min` RTT, pace at `BtlBw`, window about `2 x BDP` for headroom -
and **LEDBAT-shaped**, targeting a few milliseconds of one-way queuing delay
with no bandwidth estimate at all. Given that the goal is latency first, and
that the delay branch is the one we never explored properly, try that one
first.

The first experiment worth running is the one we could not: **the same sweep
under real `netem` loss**. Every loss result above is from a model known to be
wrong. Whether any of these rules survives a lossy path is open, and loss is
where a delay-based controller and a rate-based one diverge most.

## Reference points in this repo

| | |
|---|---|
| the rule being replaced | `rammux/src/global_pool.rs`, `transit_recv_update` |
| its config and constants | `rammux/src/config.rs`, `TransitGrowth` |
| rate estimator | `rammux/src/rate.rs` |
| stream window autotune (part 1, not yours) | `rammux/src/stream/inbound.rs` |
| cluster campaign + its findings | `rammux-perf/campaign/HANDOVER.md` |
| protocol wire format | `PROTOCOL.md` |
