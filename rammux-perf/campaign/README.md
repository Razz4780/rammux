# The benchmark campaign

Three questions, one sweep:

1. Is rammux worth using, against yamux, h2 and QUIC?
2. For each protocol on each link, what is the best configuration for
   ping pong latency, and what is the best for throughput?
3. For each protocol, what is the best single configuration to ship?

## Why the sweep looks like this

**One axis, in one unit.** Every protocol here controls the same thing - how
many bytes may be in flight on the connection - and every one of them spells
it differently. yamux has a single number, h2 and QUIC have a per-stream and a
per-connection window, rammux has both of those plus a transit window that
bounds what is actually on the wire. A ladder of absolute byte values would
compare sizings; a ladder of *multiples of the link's bandwidth-delay product*
compares protocols at the same budget, and its answer carries over to links
that are not on the list.

**One run, both metrics.** The ping pong stream runs for as long as the bulk
streams do and sends one message at a time, so every run is simultaneously a
throughput measurement and a latency-under-load measurement. There is no
separate latency sweep: objective 2's two answers are read off the same
ladder, one by sorting on `latency_ms.p99` and the other on `mbps.p50`.

**Interleaved, and anchored.** Within a link the protocols' ladders are
round-robined rather than run in blocks, so that an hour of cluster drift
becomes noise instead of a systematic advantage for whichever protocol ran
first. The same rammux configuration is also run first and last in each
link's block: those two runs should be identical, and how far apart they land
is the resolution of everything else.

**TLS everywhere.** QUIC's transport is always encrypted, so an unencrypted
run of the other three would not be comparable, and the deployment this is for
is encrypted anyway.

Held constant, and therefore not answered by this campaign: 8 bulk streams, a
1 KiB ping pong message, 3 iterations, and rammux's 5 s / 15 s probe and ping
schedule.

## Running it

```bash
cd rammux-perf/campaign

# 0. Three cheap checks first: the plumbing, then Chaos Mesh injecting, then
#    QUIC over a shaped link. Five minutes, and it is where a wrong image or
#    a missing RBAC verb should surface.
./gen.py --image "$IMAGE" --out smoke --smoke
KUBECONFIG=bench.kubeconfig ./run.sh smoke

# 1. Configs. --links narrows it; the default is all five.
./gen.py --image "$IMAGE" --out runs --links wan

# 2. The runs. Resumable: a config whose log already exists is skipped, so
#    this can be interrupted and restarted at no cost beyond the run in
#    flight. A failing run does not stop the campaign.
KUBECONFIG=bench.kubeconfig RAMMUX_PERF=./rammux-perf ./run.sh runs

# 3. The handover: every run reduced to its distributions.
./summarize.py --runs runs --logs results/logs --out results/summary.jsonl
```

`summary.jsonl` is one line per run and a few hundred kilobytes for the whole
campaign - small enough to commit. The raw logs are not: they stay in
`results/logs/`, in case a question comes up that a dozen quantiles cannot
answer.

## What comes back

Per run: the muxer config it ran, the workload, one entry per iteration, and
mean / sd / twelve quantiles for throughput, bulk elapsed time and ping pong
latency, taken over every sample in the run at once. Also `complete`, which is
false when the run did not finish every iteration it was asked for, and
`retries`, which counts iterations that lost their connection and had to start
again. A configuration that cannot finish the workload inside the timeout is a
result, not an accident - it is what a window too small for the link looks
like - so those runs are kept rather than discarded.
