#!/usr/bin/env python3
"""Turns a directory of raw job logs into one compact file to hand over.

The raw logs are large - a run on a 1 ms link logs one line per ping pong
exchange and there are tens of thousands of them - and most of that bulk is
not needed to answer the campaign's questions. This reduces each run to its
distributions: a dozen quantiles per metric rather than every sample, which is
enough to compare configurations, see the shape of a tail, and tell a real
difference from noise. The raw logs stay where they are for anything the
quantiles cannot answer.

Percentiles are taken over every sample in the run at once, never per
iteration and then combined - a percentile of percentiles describes nothing.

Usage:
    ./summarize.py --runs runs --logs results/logs --out results/summary.jsonl
"""

import argparse
import json
import math
import pathlib

# Sparse at the middle, dense at the top: the interesting part of a latency
# distribution under load is its tail, and the interesting part of a
# throughput distribution is the slowest stream.
QUANTILES = [0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 99.9, 100]


def summarize(values):
    """Mean, spread and quantiles of one metric, by nearest rank."""
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    n = len(ordered)
    mean = sum(ordered) / n
    variance = sum((value - mean) ** 2 for value in ordered) / n
    out = {"n": n, "mean": round(mean, 4), "sd": round(math.sqrt(variance), 4)}
    for quantile in QUANTILES:
        rank = max(1, min(n, math.ceil(quantile / 100 * n)))
        out[f"p{quantile:g}"] = round(ordered[rank - 1], 4)
    return out


def parse(log):
    """Reads one job log into its samples, its iterations, and its complaints.

    Deliberately forgiving: the log also carries the runtime's own output and,
    for a run that failed, a panic or a partial line. Anything that does not
    parse is skipped rather than fatal, because a partial log from a run that
    timed out is exactly the log worth reading.
    """
    bulk_mbps, bulk_ms, latency_ms, iterations, complaints = [], [], [], [], []
    for line in log.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        fields = entry.get("fields") or entry
        message = fields.get("message")
        level = entry.get("level", "INFO")
        if level in ("WARN", "ERROR"):
            complaints.append({k: v for k, v in fields.items() if k != "time"})
        if message == "Bulk stream finished":
            bulk_mbps.append(fields["mbps"])
            bulk_ms.append(fields["elapsed_us"] / 1e3)
        elif message == "Ping pong exchange finished":
            latency_ms.append(fields["elapsed_us"] / 1e3)
        elif message == "Iteration finished":
            iterations.append({key: fields[key] for key in
                               ("iteration", "attempt", "elapsed_ms", "cpu_ms",
                                "bulk_streams", "ping_pong_count")
                               if key in fields})
    return bulk_mbps, bulk_ms, latency_ms, iterations, complaints


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", default="runs", type=pathlib.Path)
    parser.add_argument("--logs", default="results/logs", type=pathlib.Path)
    parser.add_argument("--out", default="results/summary.jsonl", type=pathlib.Path)
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for log_path in sorted(args.logs.glob("*.log")):
        name = log_path.stem
        link, protocol, point = name.split("__", 2)
        config_path = args.runs / f"{name}.json"
        config = json.loads(config_path.read_text()) if config_path.exists() else {}
        bulk_mbps, bulk_ms, latency_ms, iterations, complaints = parse(log_path.read_text())
        rows.append({
            "name": name,
            "link": link,
            "protocol": protocol,
            "point": point,
            "muxer": config.get("muxer_config"),
            "workload": {
                "iterations_requested": config.get("iterations"),
                "bulk_streams": config.get("bulk_streams"),
                "bulk_stream_data_mib": config.get("bulk_stream_data"),
                "ping_pong_size": config.get("ping_pong_size"),
                "tls": config.get("tls"),
            },
            "iterations": iterations,
            # A run whose iterations are fewer than requested hit the timeout;
            # one whose attempts are above 1 lost a connection. Both change how
            # much the numbers below are worth, so they travel with them.
            "complete": len(iterations) == config.get("iterations"),
            "retries": sum(1 for it in iterations if it.get("attempt", 1) > 1),
            "mbps": summarize(bulk_mbps),
            "bulk_elapsed_ms": summarize(bulk_ms),
            "latency_ms": summarize(latency_ms),
            "complaints": complaints[:20],
        })

    with args.out.open("w") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    incomplete = [row["name"] for row in rows if not row["complete"]]
    print(f"{len(rows)} runs -> {args.out} ({args.out.stat().st_size // 1024} KiB)")
    if incomplete:
        print(f"{len(incomplete)} did not finish every iteration:")
        for name in incomplete:
            print(f"  {name}")


if __name__ == "__main__":
    main()
