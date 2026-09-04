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
| rammux | `off-open`, `off-tuned`, `transit-1x/2x/4x/8x`, `transit-auto`, `transit-auto-probe-30s` |
| h2 | `adaptive`, `fixed-1x/2x/4x/8x` |
| quic | `fixed-1x/2x/8x` |
| yamux | `global-25mib`, `global-64mib` |

20 runs a link, 100 over the five impaired links.

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

# One link first. ~45 min.
./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig --links wan

# The rest. ~2.7 h. Skips whatever already succeeded.
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

`results/results.jsonl`, one line per run: the config it ran, and the six
numbers `k8s run` reports - `failures`, `mean_bulk_elapsed_us`, mean/p50/p99
ping pong latency, `completed_ping_pongs` - plus `mbps` derived from the bulk
elapsed time. Small enough to commit. Each run's stderr is kept beside it in
`results/stderr/`, and the exact config in `results/configs/`, so a surprising
row can be re-run by hand.
