# The benchmark campaign

`bench.py` runs a matrix of `rammux-perf k8s run` invocations and records what
each one measured. Three questions:

1. Is rammux worth using, against yamux, h2 and QUIC?
2. For each protocol on each link, what is the best configuration for ping
   pong latency, and what is the best for throughput?
3. For each protocol, what is the best single configuration to ship?

## How the matrix is built

**One axis, in one unit.** Every protocol here controls the same thing - how
many bytes may be in flight on the connection - and each spells it
differently. A ladder of absolute byte values would compare sizings; a ladder
of *multiples of the link's bandwidth-delay product* compares protocols at the
same budget, and its answer carries over to links not in the list.

**One run, both metrics.** The ping pong stream runs for as long as the bulk
streams do and sends one message at a time, so every run is a throughput
measurement and a latency-under-load measurement together. Objective 2's two
answers come off the same rows, sorted two different ways.

**Interleaved, and anchored three times.** Protocols are round-robined within
a link rather than run in blocks, so an hour of cluster drift becomes noise
instead of an advantage for whichever ran first. One rammux configuration runs
at the start, the middle and the end of each link's block; how far those three
land apart is the resolution of every other difference in it.

| protocol | points per link |
|---|---|
| rammux | `transit-1x/2x/4x/8x`, each at `probe-10s` and `probe-30s` |
| h2 | `adaptive`, `fixed-256kb` |
| quic | `fixed-1x/2x/4x/8x` |
| yamux | `global-25mib` |

17 runs a link, 85 over the five impaired links.

Every protocol gets the same memory ceiling: 25 MiB of receive buffer across
the connection, which is yamux's floor (256 KiB x 100 streams) and so the
lowest budget all four can be held to. rammux's share is accounted against
the workload's 9 streams - 9 x 256 KiB of stream window plus a pool of
25 MiB minus that - because its global window is a pool on top of the
per-stream windows rather than a cap over them.

rammux's transit window is always on: the ladder asks how big it should be,
not whether it should exist. Flow control that works the other way round -
receive windows alone - is what yamux, h2 and QUIC are in the matrix for.

rammux's probe interval is the second axis, swept on the autotuning point
because that is where the probe is both the cost and the value: it stalls the
connection on both sides, which lands in the latency tail, and it is what
produces the clean round trip the transit window sizes itself from. The ping
interval is not swept - it stays at 5 s, below the probe interval, which the
schedule assumes (a refused probe backs off by one ping interval).

Held constant, and so not answered here: 8 bulk streams, a 1 KiB ping pong
message, TLS everywhere, and a 5 s ping interval.

## Running it

```bash
cd rammux-perf/campaign
IMAGE=europe-west1-docker.pkg.dev/$PROJECT/rammux/rammux-perf:dev

# Three cheap checks: plumbing, then Chaos Mesh injecting, then QUIC over a
# shaped link. ~5 min, and where a wrong image or a missing RBAC verb shows up.
./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig --smoke

# One link first.
./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig --links wan

# The rest. Skips whatever already succeeded.
./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig

# Print a results file without running anything.
./bench.py --report results/results.jsonl
```

Stopping it with Ctrl-C waits for the run in flight to tear its cluster down
rather than killing it, and re-running the same command picks up from there. A
run that fails twice is recorded as failed and the matrix moves on - some are
expected to fail, since a window sized at one BDP on a lossy link may not
finish the workload inside the timeout, and that is a result rather than an
accident.

## What comes out

`results/results.jsonl`, one line per run: the config it ran, and the numbers
`k8s run` reports - `failures`, `mean_bulk_elapsed_us`, mean/p50/p99 ping pong
latency, `completed_ping_pongs`, `total_time_us` and `total_cpu_time_us` -
plus `mbps` derived from the bulk elapsed time and `cpu_ratio`, which is CPU
over wall time. The client runs on one thread, so a `cpu_ratio` near 1.0 means
that run was CPU-bound rather than link-bound, which changes what its
throughput number means. CPU time comes from `/proc/self/stat` in USER_HZ
ticks, so it moves in 10 ms steps whatever the microseconds suggest. Small enough to commit. Each run's stderr is kept beside it in
`results/stderr/`, and the exact config in `results/configs/`, so a surprising
row can be re-run by hand.
