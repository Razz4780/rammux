#!/usr/bin/env python3
"""Runs a matrix of cluster benchmarks and collects what each one measured.

One run is one `rammux-perf k8s run`: a namespace, an echo server, an
impaired link, a client job, and six numbers on stdout. This works through a
matrix of them, records each result as it arrives, and can be stopped and
restarted without losing what it already has.

The matrix is one axis, in one unit. Every protocol here controls the same
thing - how many bytes may be in flight on the connection - and each spells it
differently: yamux has a single number, h2 and QUIC have a per-stream and a
per-connection window, rammux has both plus a transit window that bounds what
is actually on the wire. A ladder of absolute byte values would compare
sizings; a ladder of multiples of the link's bandwidth-delay product compares
the protocols at the same budget, and its answer carries over to links that
are not in the list.

Each run yields both metrics at once. The ping pong stream runs for as long as
the bulk streams do and sends one message at a time, so a run is a throughput
measurement and a latency-under-load measurement together - which is why there
is no separate latency sweep. Read the throughput answer off `mbps` and the
latency answer off `p99_ping_pong_latency_us`, from the same rows.

    ./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig --smoke
    ./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig --links wan
    ./bench.py --image "$IMAGE" --kubeconfig bench.kubeconfig
    ./bench.py --report results/results.jsonl
"""

import argparse
import datetime
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time

KIB = 1024
MIB = 1024 * 1024

# Every link the campaign runs over.
#
# `bdp` is rate x RTT in bytes - the unit every window in the ladder is a
# multiple of - and comes straight from `LinkProfile::bdp_bytes` for the
# shaped links. `datacenter` is unshaped and so has no BDP of its own; the
# value below is a nominal 2 Gbit/s x 1 ms, which is the right order of
# magnitude for pod-to-pod on a mid-size node and is only ever used as the
# ladder's unit.
#
# `mib` is how much data each bulk stream moves, sized per link so an
# iteration takes roughly 15-20 s at that link's rate: long enough that
# connection setup is not what is being measured, short enough that the whole
# matrix fits in an afternoon. It also sets how many latency samples a run
# collects, since the ping pong stream stops when the last bulk stream does -
# on `lossy-wan` that is ~100 exchanges an iteration, elsewhere many more.
LINKS = {
    "none":       {"bdp": 250 * 1000, "mib": 128},
    "datacenter": {"bdp": 250 * 1000, "mib": 128},
    "wifi-vpn":   {"bdp": 250 * 1000, "mib": 24},
    "wan":        {"bdp": 1_500_000,  "mib": 48},
    "wan-vpn":    {"bdp": 1_062_500,  "mib": 24},
    "lossy-wan":  {"bdp": 1_250_000,  "mib": 16},
}
# `none` is the unimpaired control. It is not part of the sweep - a ladder
# over a link with no bottleneck measures the host - so it is opted into.
LADDER_LINKS = [link for link in LINKS if link != "none"]

# Held constant, so that the ladder is about windows and nothing else.
BULK_STREAMS = 8
PING_PONG_SIZE = 1 * KIB
ITERATIONS = 3
TLS = True
TIMEOUT_SECS = 900

# rammux's RTT schedule. Not under test, and deliberately slack: the probe
# interval doubles as the probe's deadline, and an expired probe kills the
# connection - which on a wide, slow link would be a property of the schedule
# rather than of the window being measured.
PROBE_INTERVAL = 5
PING_INTERVAL = 15

# What "receive windows are not the constraint" means for rammux, in the runs
# where the transit window is the thing being laddered.
RAMMUX_OPEN_STREAM = 1 * MIB
RAMMUX_OPEN_GLOBAL = 16 * MIB


def window(value):
    """Windows are u32 in the config, and none of them may be zero."""
    return max(64 * KIB, min(int(value), 0xFFFF_FFFF))


def rammux_ladder(bdp):
    """rammux: the transit window is the axis, receive windows are held open.

    Two zero-transit points, not one. `off-open` is rammux with the feature
    disabled and its windows left where someone who had not thought about it
    would leave them - the failure mode the transit window exists to prevent,
    and worth having in the results. `off-tuned` is rammux with the feature
    disabled and its receive windows sized to the link, which is the same flow
    control yamux and h2 have; that is the baseline the transit window has to
    beat to have earned its place.
    """
    points = [
        ("off-open", {
            "transit_window": 0,
            "transit_window_max": 0,
            "stream_recv_window": window(RAMMUX_OPEN_STREAM),
            "global_recv_window": RAMMUX_OPEN_GLOBAL,
        }),
        ("off-tuned", {
            "transit_window": 0,
            "transit_window_max": 0,
            "stream_recv_window": window(2 * bdp / 4),
            "global_recv_window": int(2 * bdp),
        }),
    ]
    for mult in (1, 2, 4, 8):
        points.append((f"transit-{mult}x", {
            # Initial and cap equal: a fixed window, so the point measures the
            # size rather than autotune's path to it.
            "transit_window": window(mult * bdp),
            "transit_window_max": window(mult * bdp),
            "stream_recv_window": window(RAMMUX_OPEN_STREAM),
            "global_recv_window": RAMMUX_OPEN_GLOBAL,
        }))
    # Starts where the smallest fixed point sits and may grow to where the
    # largest does, so it is directly comparable to both.
    points.append(("transit-auto", {
        "transit_window": window(1 * bdp),
        "transit_window_max": window(8 * bdp),
        "stream_recv_window": window(RAMMUX_OPEN_STREAM),
        "global_recv_window": RAMMUX_OPEN_GLOBAL,
    }))
    return [(name, dict(protocol="rammux", probe_interval=PROBE_INTERVAL,
                        ping_interval=PING_INTERVAL, **fields))
            for name, fields in points]


def yamux_ladder(bdp):
    """yamux: two points, because there is nowhere else to go.

    yamux exposes one number and asserts it is at least 256 KiB per stream;
    with the 100-stream limit both sides run, that is a 25 MiB floor. On every
    link here the floor is already many times the BDP - 17x on `wan` - so
    yamux cannot be sized down to the link even in principle. The second point
    is there to show whether going further up changes anything.
    """
    del bdp
    return [
        ("global-25mib", {"protocol": "yamux", "global_recv_window": 25 * MIB}),
        ("global-64mib", {"protocol": "yamux", "global_recv_window": 64 * MIB}),
    ]


def h2_ladder(bdp):
    """h2: hyper's BDP estimator, against four fixed budgets.

    `adaptive` is the interesting one - the only autotuning in the comparison
    other than rammux's, and what a real hyper deployment runs. The fixed
    points give it something to be measured against.
    """
    points = [("adaptive", {
        "protocol": "h2", "adaptive_window": True,
        # Ignored when adaptive is on, but the fields are not optional.
        "stream_recv_window": window(bdp), "global_recv_window": window(2 * bdp),
    })]
    for mult in (1, 2, 4, 8):
        points.append((f"fixed-{mult}x", {
            "protocol": "h2", "adaptive_window": False,
            "stream_recv_window": window(mult * bdp / 4),
            "global_recv_window": window(mult * bdp),
        }))
    return points


def quic_ladder(bdp):
    """QUIC: three points, spread wide rather than dense.

    quinn runs its own congestion controller, so bytes in flight are bounded
    by that whatever the receive windows say; the windows only have to be
    large enough not to be the binding constraint. A dense ladder would
    measure the same thing three times, so this one brackets it instead.
    """
    return [(f"fixed-{mult}x", {
        "protocol": "quic",
        "stream_recv_window": window(mult * bdp / 4),
        "global_recv_window": window(mult * bdp),
        "max_streams": 100,
    }) for mult in (1, 2, 8)]


LADDERS = {
    "rammux": rammux_ladder,
    "h2": h2_ladder,
    "quic": quic_ladder,
    "yamux": yamux_ladder,
}

# Three cheap runs that between them exercise everything a real one needs, so
# a wrong image or a missing RBAC verb costs five minutes rather than being
# found an hour into the matrix. In order: the plumbing with no impairment,
# then Chaos Mesh actually injecting, then QUIC - UDP, and so the one protocol
# whose behaviour under netem and a bandwidth cap cannot be assumed from the
# others.
SMOKE = [
    ("none", "rammux", "smoke", {
        "protocol": "rammux", "probe_interval": PROBE_INTERVAL,
        "ping_interval": PING_INTERVAL, "transit_window": 256 * KIB,
        "transit_window_max": 256 * KIB, "stream_recv_window": 256 * KIB,
        "global_recv_window": 4 * MIB,
    }),
    ("datacenter", "h2", "smoke", {
        "protocol": "h2", "adaptive_window": True,
        "stream_recv_window": 256 * KIB, "global_recv_window": 1 * MIB,
    }),
    ("wan", "quic", "smoke", {
        "protocol": "quic", "stream_recv_window": 512 * KIB,
        "global_recv_window": 2 * MIB, "max_streams": 100,
    }),
]


def matrix(links, protocols):
    """The runs to do, in the order to do them.

    Round-robined across protocols within a link rather than run in blocks. A
    cluster is not a constant - nodes pick up noisy neighbours, and an hour in
    the machine is not the machine it was - and running one protocol's whole
    ladder and then the next would fold that drift into the protocol
    comparison, which is the one comparison this exists to make. Interleaving
    spreads every protocol across the whole window, so drift becomes noise
    instead of bias.

    The same rammux configuration also runs first and last in each link's
    block. Those two runs should be identical; how far apart they land is the
    resolution of every other difference in that block.
    """
    runs = []
    for link in links:
        bdp = LINKS[link]["bdp"]
        ladders = {name: LADDERS[name](bdp) for name in protocols}
        block = []
        for index in range(max(len(points) for points in ladders.values())):
            for protocol in protocols:
                points = ladders[protocol]
                if index < len(points):
                    block.append((link, protocol, *points[index]))
        anchor = next((run for run in block
                       if run[1] == "rammux" and run[2] == "transit-2x"), None)
        if anchor:
            # Distinct names, or the two would collide with the ladder's own
            # `transit-2x` and with each other - and a run is skipped on a
            # restart by name, so a collision would quietly drop the anchor.
            # The ladder's own copy sits somewhere in the middle, which makes
            # three readings of one configuration across the block.
            block = ([(link, "rammux", "transit-2x-first", anchor[3])]
                     + block
                     + [(link, "rammux", "transit-2x-last", anchor[3])])
        runs.extend(block)
    return runs


def config_for(link, muxer, image, iterations, smoke=False):
    # A smoke run is checking that the plumbing works, not measuring anything,
    # so it moves as little data as it can get away with. Inheriting the
    # link's own sizing would make the "cheap check" a gigabyte.
    streams, mib = (2, 4) if smoke else (BULK_STREAMS, LINKS[link]["mib"])
    return {
        "image": image,
        "archetype": link,
        "muxer_config": muxer,
        "iterations": iterations,
        "bulk_streams": streams,
        "bulk_stream_data": mib,
        "ping_pong_size": PING_PONG_SIZE,
        "tls": TLS,
        "timeout_secs": 300 if smoke else TIMEOUT_SECS,
    }


# The six lines `k8s run` prints on stdout. Everything else it has to say goes
# to stderr, so stdout is exactly this and nothing else.
REPORT_FIELDS = {
    "FAILURES": "failures",
    "MEAN BULK ELAPSED": "mean_bulk_elapsed_us",
    "MEAN PING PONG LATENCY": "mean_ping_pong_latency_us",
    "P50 PING PONG LATENCY": "p50_ping_pong_latency_us",
    "P99 PING PONG LATENCY": "p99_ping_pong_latency_us",
    "COMPLETED PING PONG": "completed_ping_pongs",
}
# The unit suffix is a mu, and not worth depending on.
REPORT_LINE = re.compile(r"^([A-Z0-9 ]+?):\s*(\d+)")


def parse_report(stdout):
    """Reads the six numbers back, or says which are missing."""
    found = {}
    for line in stdout.splitlines():
        match = REPORT_LINE.match(line.strip())
        if match and match.group(1) in REPORT_FIELDS:
            found[REPORT_FIELDS[match.group(1)]] = int(match.group(2))
    missing = set(REPORT_FIELDS.values()) - set(found)
    if missing:
        return None, f"stdout had no {', '.join(sorted(missing))}"
    return found, None


def mbps(bytes_per_stream, elapsed_us):
    """Bits over microseconds is megabits over seconds, the 1e6 cancelling.

    Per stream, and the elapsed time includes reading the echo back, so this
    is one stream's goodput rather than what the link carried. It is
    comparable across every row here, which is what it is for.
    """
    if not elapsed_us:
        return None
    return round(bytes_per_stream * 8 / elapsed_us, 2)


def run_one(binary, config_path, env, timeout):
    """One `k8s run`, with its own Ctrl-C left to it.

    The binary tears the cluster down on SIGINT, and a Ctrl-C at the terminal
    reaches it directly through the process group. So an interrupt here waits
    for the child rather than exiting out from under it - killing it would
    leave a namespace, an impaired link, and a bill.
    """
    started = time.monotonic()
    process = subprocess.Popen(
        [binary, "k8s", "run", "--config-path", str(config_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    interrupted = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except KeyboardInterrupt:
        interrupted = True
        print("\n  interrupted - waiting for the run to tear its cluster down", flush=True)
        try:
            stdout, stderr = process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
    except subprocess.TimeoutExpired:
        # Past the binary's own timeout, so it is not coming back on its own.
        process.send_signal(signal.SIGINT)
        try:
            stdout, stderr = process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
        return None, "the run outlived its own timeout", time.monotonic() - started, stderr
    elapsed = time.monotonic() - started
    if interrupted:
        raise KeyboardInterrupt
    if process.returncode != 0:
        return None, f"exit {process.returncode}", elapsed, stderr
    report, error = parse_report(stdout)
    return report, error, elapsed, stderr


def load_done(path):
    """Names already recorded as successful, so a restart skips them."""
    done = set()
    if not path.exists():
        return done
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("status") == "ok":
            done.add(row["name"])
    return done


def report(path):
    """Prints what is in a results file, grouped the way it is read."""
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    rows = [row for row in rows if row.get("status") == "ok"]
    if not rows:
        print(f"no successful runs in {path}")
        return
    header = f"{'point':<22} {'mbit/s':>9} {'p50 ms':>9} {'p99 ms':>9} {'mean ms':>9} {'pp n':>7} {'fail':>5}"
    for link in dict.fromkeys(row["link"] for row in rows):
        print(f"\n=== {link}")
        for protocol in dict.fromkeys(row["protocol"] for row in rows if row["link"] == link):
            print(f"\n  {protocol}\n  {header}")
            block = [row for row in rows
                     if row["link"] == link and row["protocol"] == protocol]
            for row in block:
                print("  {:<22} {:>9} {:>9.3f} {:>9.3f} {:>9.3f} {:>7} {:>5}".format(
                    row["point"],
                    row["mbps"] if row["mbps"] is not None else "-",
                    row["p50_ping_pong_latency_us"] / 1e3,
                    row["p99_ping_pong_latency_us"] / 1e3,
                    row["mean_ping_pong_latency_us"] / 1e3,
                    row["completed_ping_pongs"],
                    row["failures"],
                ))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--image", help="container image with rammux-perf as its entrypoint")
    parser.add_argument("--kubeconfig", type=pathlib.Path,
                        help="kubeconfig for the bench cluster (default: the ambient one)")
    parser.add_argument("--binary", default="rammux-perf", help="the rammux-perf to run")
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("results"),
                        help="directory for the configs, the results and the stderr")
    parser.add_argument("--links", nargs="*", default=None, choices=list(LINKS),
                        help=f"default: {' '.join(LADDER_LINKS)}")
    parser.add_argument("--protocols", nargs="*", default=list(LADDERS),
                        choices=list(LADDERS))
    parser.add_argument("--iterations", type=int, default=ITERATIONS)
    parser.add_argument("--retries", type=int, default=2,
                        help="attempts per run before it is recorded as failed")
    parser.add_argument("--smoke", action="store_true",
                        help="the three cheap checks instead of the matrix")
    parser.add_argument("--redo", action="store_true",
                        help="re-run everything, rather than skipping what already succeeded")
    parser.add_argument("--dry-run", action="store_true",
                        help="write the configs and print the plan, run nothing")
    parser.add_argument("--report", type=pathlib.Path,
                        help="print an existing results file and exit")
    args = parser.parse_args()

    if args.report:
        report(args.report)
        return 0
    if not args.image:
        parser.error("--image is required (or use --report)")

    runs = SMOKE if args.smoke else matrix(args.links or LADDER_LINKS, args.protocols)
    iterations = 1 if args.smoke else args.iterations

    out = args.out
    (out / "configs").mkdir(parents=True, exist_ok=True)
    (out / "stderr").mkdir(parents=True, exist_ok=True)
    results = out / ("smoke.jsonl" if args.smoke else "results.jsonl")
    done = set() if args.redo else load_done(results)

    env = dict(os.environ)
    env.setdefault("RUST_LOG", "info")
    if args.kubeconfig:
        env["KUBECONFIG"] = str(args.kubeconfig.resolve())
    elif "KUBECONFIG" not in env:
        print("note: no --kubeconfig and none in the environment; "
              "the run will use whatever kube can infer", file=sys.stderr)

    planned = []
    for link, protocol, point, muxer in runs:
        name = f"{link}__{protocol}__{point}"
        config = config_for(link, muxer, args.image, iterations, smoke=args.smoke)
        (out / "configs" / f"{name}.json").write_text(json.dumps(config, indent=2) + "\n")
        planned.append((name, link, protocol, point, config))

    todo = [run for run in planned if run[0] not in done]
    print(f"{len(planned)} runs, {len(planned) - len(todo)} already done, {len(todo)} to go")
    if args.dry_run:
        for name, *_ in planned:
            print(f"  {'skip' if name in done else 'run '} {name}")
        return 0
    if not todo:
        report(results)
        return 0
    print(f"roughly {len(todo) * 2 // 60}h{len(todo) * 2 % 60:02d}m at ~2 min a run\n")

    failed = []
    try:
        for index, (name, link, protocol, point, config) in enumerate(todo, start=1):
            config_path = out / "configs" / f"{name}.json"
            for attempt in range(1, args.retries + 1):
                print(f"[{index:3d}/{len(todo)}] {name:<40} try {attempt} ... ",
                      end="", flush=True)
                # Generous against the binary's own timeout: it would rather
                # give up and tear down itself, and only a wedged run should
                # ever reach this.
                report_fields, error, elapsed, stderr = run_one(
                    args.binary, config_path, env, config["timeout_secs"] + 600)
                (out / "stderr" / f"{name}.log").write_text(stderr)
                row = {
                    "name": name, "link": link, "protocol": protocol, "point": point,
                    "at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "duration_s": round(elapsed, 1),
                    "attempt": attempt,
                    "config": config,
                }
                if error:
                    print(f"FAILED ({elapsed:.0f}s): {error}")
                    for line in stderr.strip().splitlines()[-2:]:
                        print(f"          {line[:160]}")
                    row["status"] = "failed"
                    row["error"] = error
                else:
                    row["status"] = "ok"
                    row.update(report_fields)
                    row["mbps"] = mbps(config["bulk_stream_data"] * MIB,
                                       report_fields["mean_bulk_elapsed_us"])
                    print(f"ok ({elapsed:.0f}s) "
                          f"{row['mbps']} mbit/s, "
                          f"p50 {row['p50_ping_pong_latency_us'] / 1e3:.2f} ms, "
                          f"p99 {row['p99_ping_pong_latency_us'] / 1e3:.2f} ms")
                with results.open("a") as handle:
                    handle.write(json.dumps(row) + "\n")
                if row["status"] == "ok":
                    break
                if attempt == args.retries:
                    failed.append(name)
                else:
                    # Often a namespace still terminating, or an API server
                    # that was briefly unhappy. Both pass.
                    time.sleep(15)
    except KeyboardInterrupt:
        print("\nstopped. Re-run the same command to pick up where this left off.")
        return 130

    print(f"\n{len(todo) - len(failed)}/{len(todo)} runs produced a result -> {results}")
    if failed:
        print("failed:")
        for name in failed:
            print(f"  {name}  (stderr in {out / 'stderr' / (name + '.log')})")
    report(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
