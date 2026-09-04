# Handover

Where the benchmark campaign stands, what has been established, and what is
still open. Written so a session with no history can pick this up: read this,
then `README.md` for how to run it and `bench.py` for the design.

Branch: `research/transit-window`. **No cluster results exist yet** - the
tooling is built and verified locally, nothing has been run on GKE.

---

## The split of work

The sandbox this was built in cannot reach the cluster: its egress policy
blocks the GKE API endpoint, every container-registry blob CDN, and
`chaos-mesh.org`. Its kernel has no `sch_netem` either, so no local link
emulation. **Someone with cluster access runs the benchmarks and brings the
results back**; the analysis happens here.

---

## The contract that holds it together

One format, in four places. Break it in one and the others fail somewhere
unhelpful.

1. `client.rs` logs a single `Finished all iterations` line, with
   `iterations`, `failures`, `mean_bulk_elapsed_micros`, mean/p50/p99
   `ping_pong_latency_micros`, `completed_ping_pongs`, `total_time_micros`,
   `total_cpu_time_micros`. The comment above it says "This log is expected,
   in this format" - that is what it means.
2. `k8s.rs`'s `RunReport::from_logs` deserializes that line out of the job's
   logs. Every field is required: drop one from the client and the run fails
   with "final run report was not found in the client logs", which does not
   point at the real cause.
3. `k8s run` prints those numbers as labelled lines on stdout. Its own
   tracing goes to stderr, so stdout is exactly the report and nothing else.
4. `bench.py`'s `REPORT_FIELDS` maps those labels back to row keys.

`tests/e2e.rs` pins step 1 against steps 2-4 for all four protocols. Run it
after touching any of them.

---

## Established, with evidence - do not re-derive

**Chaos Mesh resolves its selectors once, at injection, and cannot inject into
a pod that does not exist yet.** This is why the run order is: server up,
client job created and held at the start gate, chaos injected into both pods,
gate opened. Get it wrong and the run silently measures a clean link. The gate
is a Service whose name does not resolve until it is created; expect a few
seconds' lag after it appears, because CoreDNS caches the denial for 5 s.

**QUIC's configuration is a transport parameter**, fixed in the handshake, so
the server must know it before the client says anything. `server_config()`
copies the muxer config into the server's `quic` block, and the server refuses
a connection whose config differs - `closed by peer: QUIC config mismatch`. A
QUIC run against a server configured for different windows fails outright; it
does not silently use the server's.

**rammux's `global_recv_window` is a pool streams borrow from, on top of their
own `stream_recv_window`** - not a cap over them (`rammux/src/config.rs`).
That is why the 25 MiB budget is written as `25 MiB - 256 KiB * 9`: nine
streams at 256 KiB plus the remaining pool comes to exactly 25 MiB for this
workload. yamux and h2 spell the same budget as a cap, so theirs is the plain
25 MiB.

**The transit window autotunes by doubling**, once per window update (an
update is due when half the current window has been freed), ceilinged at
`2 x clean_rtt x rate_ema` and clamped to `transit_window_max`. Grow-only.
`GlobalPool::transit_recv_update` in the library. Consequence: the ceiling is
about twice the link's BDP, and it is reached from a small initial window in
roughly five doublings costing about one final-window's worth of data.

**The probe and the ping are not interchangeable.** A link-clearing probe
pauses data output on *both* sides for a round trip, so it must be rare
(>= 10 s); a plain ping holds nothing back but the next probe, so it can be
frequent (5 s). The ordering is structural: a probe refused because one is
already running backs off by `ping_every`, so a ping interval above the probe
interval would back off for longer than the probe period. The clean RTT the
probe measures sizes the transit window; the loaded RTT the ping measures
sizes per-stream receive windows.

**CPU time has 10 ms granularity.** `cpu.rs` reads `/proc/self/stat` in
USER_HZ ticks, however many microseconds it is reported in.

**Kubernetes details that cost time.** `deletecollection` is a distinct RBAC
verb from `delete` - teardown deletes by label selector, so granting only
`delete` leaves every run's objects behind. `pods/log` is its own subresource.
k8s-openapi ignores unknown fields when deserializing, so a mistyped key in a
resource is silently dropped rather than rejected.

**CLI shape.** `--json-log` is a global flag, before the subcommand
(`rammux-perf --json-log k8s run ...`). `RUST_LOG=info` is required or the
subscriber drops everything and the run produces no output at all. Cargo
invocations in this project use `RUSTUP_TOOLCHAIN=1.95.0`.

---

## Bugs found and fixed - do not reintroduce

* `percentile` takes a percentage. It was being called with `0.50` and `0.99`,
  which is the 0.5th and 0.99th percentile - on any sample under 200, the
  fastest exchange in the run, twice. A local run reported p50 and p99 both
  217us against a 1795us mean; corrected, 676us and 4492us. Pinned by
  `client::test::percentiles_are_percentages`.
* `mean` of an empty sample divided by a zero length, which is a NaN, which
  `Duration` panics on. Reached by any run configured without a ping pong
  stream.
* The drift anchors stopped being generated when the ladder's point names
  changed, because `matrix` found the anchor by matching a name. Nothing
  failed - the campaign just quietly lost the repeated runs that say how big a
  difference has to be before it means anything. `ANCHOR` is now a named
  constant and `matrix` refuses to build a plan when it does not name a real
  point.

The shape of all three: a wrong number that still looks like a right number.
Prefer a test or a hard failure over a comment when the failure mode is
silence.

---

## Open questions on the current matrix

Design decisions taken deliberately, but worth revisiting before the results
are read as more than they are.

1. **The transit ladder does not vary the transit window in steady state.**
   `transit_window_max` is 16 MiB on every point while `transit_window` varies
   64 KiB to 512 KiB, so autotune is live on all four and all four converge to
   the same `2 x BDP` ceiling. Getting there costs about 3 MB of the 64 MiB an
   iteration moves, so the four points differ by roughly a 5% ramp rather than
   by window size. A real size sweep needs `transit_window_max` to move with
   `transit_window`.
2. **The ladder is absolute, not BDP-relative.** `8x` is 512 KiB everywhere,
   which is 2x the BDP on `wifi-vpn` and 0.4x on `lossy-wan` - where every
   point starts below one BDP. The multiplier means a different thing on each
   link.
3. **h2 and QUIC realize less buffer than the 25 MiB ceiling allows.** Their
   per-stream window binds first (9 x 256 KiB = 2.25 MiB for h2-fixed), while
   rammux can borrow up to the full pool. The fair autotuning comparison is
   rammux against `h2 adaptive`; the fixed points are floors.
4. **The unimpaired control is unreachable.** `--links` no longer offers
   `none`, so the `none` archetype only appears in the smoke batch.
5. **Iteration length is uniform.** `bulk_stream_data` is 8 MiB on every link,
   so an iteration is ~10 s on `lossy-wan` and well under a second on
   `datacenter`. Every iteration opens a fresh connection and pays slow start,
   which is consistent across protocols but is included in every number.

---

## Next steps

1. Build and push the image; apply the RBAC (namespace, SA, Role with
   `deletecollection` and `pods/log`, plus a cluster-scoped namespace `get`).
2. `./bench.py --image "$IMAGE" --kubeconfig ... --smoke` - three runs, ~5 min.
   Plumbing, then Chaos Mesh injecting, then QUIC over a shaped link.
3. `--links wan` - one link, 17 runs. **Stop and analyse here.** Check the
   ladder is centred rather than won at an endpoint, and check how far the
   three anchor runs (`transit-2x-probe-10s-first`, the ladder's own copy, and
   `-last`) land apart. That spread is the resolution of every other
   difference; if it is wider than the differences, the matrix needs more
   iterations before it is worth running in full.
4. The remaining four links, then a head-to-head of each protocol's best
   configuration across all links with more iterations.

Results land in `results/results.jsonl`, one line per run - small enough to
commit. `./bench.py --report results/results.jsonl` prints it.

---

## A note on this session's transcript

It exists at `~/.claude/projects/<escaped-cwd>/<session-id>.jsonl` on the
machine that ran it, and `claude --resume <session-id>` restores it. It is 31
MB, which is far past any context window, so a resume re-reads and immediately
compacts it. **It also contains a GKE service-account token, a client key and
CA data** that were pasted into the conversation - do not commit it to this
repository or share it. This file exists so none of that is necessary.
