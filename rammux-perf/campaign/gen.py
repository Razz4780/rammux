#!/usr/bin/env python3
"""Generates the config files for one stage of the benchmark campaign.

The whole design of the campaign lives in this file: which links, which
configurations of each protocol, how much work each run does, and in what
order the runs happen. Nothing else needs editing to change the sweep.

The sweep is over *one* axis, expressed in the same unit for every protocol:
how many bytes the protocol will allow in flight on the connection, as a
multiple of the link's bandwidth-delay product. Every protocol here spells
that differently - yamux has one number for it, h2 and QUIC have two, rammux
has a receive window and a separate transit window - so a ladder of absolute
byte values would compare sizings rather than protocols. A ladder of BDP
multiples compares the protocols at the same budget, and its answer
generalises to links that are not in the list.

Each run yields both metrics at once: the ping pong stream runs for as long
as the bulk streams do, so one run is a throughput sample and a
latency-under-load sample. The latency-optimal and throughput-optimal
configurations therefore come out of the same ladder, which is why there is
no separate latency sweep.

Usage:
    ./gen.py --image <ref> --out runs [--links wan] [--stage ladder]
"""

import argparse
import json
import pathlib

KIB = 1024
MIB = 1024 * 1024

# Every link the campaign runs over.
#
# `bdp` is rate x RTT, in bytes - the number every window in the ladder is a
# multiple of. It comes straight from LinkProfile::bdp_bytes for the shaped
# links. `datacenter` is unshaped, so it has no BDP of its own; the value
# below is a nominal 2 Gbit/s x 1 ms, which is the right order of magnitude
# for pod-to-pod on a mid-size node and is only ever used as the ladder's
# unit.
#
# `mib` is how much data each bulk stream moves. It is sized per link so that
# an iteration takes roughly 15-20 s at the link's rate: long enough that
# connection setup is not what is being measured, short enough that 90 runs
# fit in an afternoon. It also decides how many latency samples a run
# collects, since the ping pong stream stops when the last bulk stream does -
# on `lossy-wan` that is ~100 exchanges per iteration, everywhere else more.
LINKS = {
    "datacenter": {"bdp": 250 * 1000, "mib": 128, "nominal_bdp": True},
    "wifi-vpn":   {"bdp": 250 * 1000, "mib": 24},
    "wan":        {"bdp": 1_500_000,  "mib": 48},
    "wan-vpn":    {"bdp": 1_062_500,  "mib": 24},
    "lossy-wan":  {"bdp": 1_250_000,  "mib": 16},
}

# Held constant across the campaign, so that the ladder is about windows.
BULK_STREAMS = 8
PING_PONG_SIZE = 1 * KIB
ITERATIONS = 3
TLS = True
# Generous, because a run that hits this produces no throughput number. It is
# not a target: it is the point at which a configuration is declared unable to
# finish the workload on this link, which is itself a result.
TIMEOUT_SECS = 900

# rammux's RTT schedule. Not under test, and deliberately slack: the probe
# interval doubles as the probe's deadline, and a probe that expires kills the
# connection - which on a wide, slow link is a property of the schedule rather
# than of the window being measured.
PROBE_INTERVAL = 5
PING_INTERVAL = 15

# What "receive windows are not the constraint" means for rammux, in the runs
# where the transit window is the thing being laddered.
RAMMUX_GENEROUS_STREAM = 1 * MIB
RAMMUX_GENEROUS_GLOBAL = 16 * MIB


def clamp32(value):
    """Windows are u32 in the config, and none may be zero."""
    return max(64 * KIB, min(int(value), 0xFFFF_FFFF))


def rammux_ladder(bdp):
    """rammux: the transit window is the axis, receive windows are held open.

    Two zero-transit points, not one. `off-generous` is rammux with the
    feature disabled and the windows left where a person who had not thought
    about it would leave them - that is the failure mode the transit window
    exists to prevent, and it belongs in the results. `off-tuned` is rammux
    with the feature disabled and its receive windows sized to the link, which
    is the same flow control yamux and h2 have; that is the honest baseline
    the transit window has to beat to have earned its place.
    """
    points = [
        ("off-generous", {
            "transit_window": 0,
            "transit_window_max": 0,
            "stream_recv_window": clamp32(RAMMUX_GENEROUS_STREAM),
            "global_recv_window": RAMMUX_GENEROUS_GLOBAL,
        }),
        ("off-tuned", {
            "transit_window": 0,
            "transit_window_max": 0,
            "stream_recv_window": clamp32(2 * bdp / 4),
            "global_recv_window": int(2 * bdp),
        }),
    ]
    for mult in (1, 2, 4, 8):
        points.append((f"transit-{mult}x", {
            # Initial and cap equal: a fixed window, so the point measures the
            # size rather than the autotune's path to it.
            "transit_window": clamp32(mult * bdp),
            "transit_window_max": clamp32(mult * bdp),
            "stream_recv_window": clamp32(RAMMUX_GENEROUS_STREAM),
            "global_recv_window": RAMMUX_GENEROUS_GLOBAL,
        }))
    points.append(("transit-auto", {
        # Starts where the smallest fixed point sits and is allowed to grow to
        # where the largest does, so it is directly comparable to both.
        "transit_window": clamp32(1 * bdp),
        "transit_window_max": clamp32(8 * bdp),
        "stream_recv_window": clamp32(RAMMUX_GENEROUS_STREAM),
        "global_recv_window": RAMMUX_GENEROUS_GLOBAL,
    }))
    return [(name, dict(protocol="rammux", probe_interval=PROBE_INTERVAL,
                        ping_interval=PING_INTERVAL, **fields))
            for name, fields in points]


def yamux_ladder(bdp):
    """yamux: two points, because there is nowhere else to go.

    yamux exposes one number, and asserts it is at least 256 KiB per stream -
    with the 100-stream limit both sides run, that is a 25 MiB floor. On every
    link here that floor is already many times the BDP (17x on `wan`), so
    yamux cannot be sized down to the link even in principle. The second point
    is there to show whether going further up changes anything.
    """
    del bdp
    return [
        ("global-25mib", {"protocol": "yamux", "global_recv_window": 25 * MIB}),
        ("global-64mib", {"protocol": "yamux", "global_recv_window": 64 * MIB}),
    ]


def h2_ladder(bdp):
    """h2: hyper's BDP estimator against four fixed budgets.

    `adaptive` is the interesting one - it is the only autotuning in the
    comparison other than rammux's, and it is what a real hyper deployment
    runs. The fixed points give it something to be measured against.
    """
    points = [("adaptive", {
        "protocol": "h2", "adaptive_window": True,
        # Ignored when adaptive is on, but the fields are not optional.
        "stream_recv_window": clamp32(bdp), "global_recv_window": clamp32(2 * bdp),
    })]
    for mult in (1, 2, 4, 8):
        points.append((f"fixed-{mult}x", {
            "protocol": "h2", "adaptive_window": False,
            "stream_recv_window": clamp32(mult * bdp / 4),
            "global_recv_window": clamp32(mult * bdp),
        }))
    return points


def quic_ladder(bdp):
    """QUIC: three points, spread wide rather than dense.

    quinn runs its own congestion controller, so bytes in flight are bounded
    by that regardless of what the receive windows say; the windows only need
    to be large enough not to be the binding constraint. A dense ladder would
    measure the same thing three times, so this one just brackets it.
    """
    return [(f"fixed-{mult}x", {
        "protocol": "quic",
        "stream_recv_window": clamp32(mult * bdp / 4),
        "global_recv_window": clamp32(mult * bdp),
        "max_streams": 100,
    }) for mult in (1, 2, 8)]


LADDERS = {
    "rammux": rammux_ladder,
    "yamux": yamux_ladder,
    "h2": h2_ladder,
    "quic": quic_ladder,
}


def interleave(per_protocol):
    """Round-robins the protocols' ladders together.

    A cluster is not a constant. Nodes get noisy neighbours, and an hour into
    a run the machine is not the machine it was at the start. Running one
    protocol's whole ladder and then the next would fold that drift into the
    protocol comparison, which is the one comparison the campaign exists to
    make. Interleaving spreads every protocol across the whole window instead,
    so drift becomes noise rather than bias.
    """
    ordered = []
    for index in range(max(len(points) for points in per_protocol.values())):
        for protocol in ("rammux", "h2", "quic", "yamux"):
            points = per_protocol[protocol]
            if index < len(points):
                ordered.append((protocol, *points[index]))
    return ordered


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--image", required=True,
                        help="container image with rammux-perf as its entrypoint")
    parser.add_argument("--out", default="runs", type=pathlib.Path,
                        help="directory to write the configs and the plan into")
    parser.add_argument("--links", nargs="*", default=list(LINKS),
                        choices=list(LINKS), help="which links to generate for")
    parser.add_argument("--iterations", type=int, default=ITERATIONS)
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    plan = []
    for link in args.links:
        spec = LINKS[link]
        bdp = spec["bdp"]
        ordered = interleave({name: ladder(bdp) for name, ladder in LADDERS.items()})
        # The same configuration first and last in the link's block, half an
        # hour or so apart. Two runs that should be identical are the only way
        # to know how big a difference between two ladder points has to be
        # before it means anything.
        anchor = next(point for point in ordered
                      if point[0] == "rammux" and point[1] == "transit-2x")
        ordered = [anchor] + ordered + [(anchor[0], "transit-2x-repeat", anchor[2])]

        for protocol, point, muxer in ordered:
            name = f"{link}__{protocol}__{point}"
            config = {
                "image": args.image,
                "archetype": link,
                "muxer_config": muxer,
                "iterations": args.iterations,
                "bulk_streams": BULK_STREAMS,
                "bulk_stream_data": spec["mib"],
                "ping_pong_size": PING_PONG_SIZE,
                "tls": TLS,
                "timeout_secs": TIMEOUT_SECS,
                "log_path": f"results/logs/{name}.log",
            }
            (args.out / f"{name}.json").write_text(json.dumps(config, indent=2) + "\n")
            plan.append(name)

    (args.out / "plan.txt").write_text("\n".join(plan) + "\n")
    minutes = len(plan) * 2
    print(f"{len(plan)} runs over {len(args.links)} link(s) -> {args.out}")
    print(f"roughly {minutes // 60}h{minutes % 60:02d}m at ~2 min per run")


if __name__ == "__main__":
    main()
