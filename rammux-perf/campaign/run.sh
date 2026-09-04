#!/usr/bin/env bash
#
# Runs a campaign, one config at a time, and can be stopped and restarted.
#
# Resumability is the point. Ninety cluster runs is three hours, and three
# hours is long enough that something will interrupt it - a laptop lid, an
# expired token, a run that hangs. A finished run leaves its log behind and is
# skipped on the next pass, so restarting costs only what was actually lost.
#
# A run that fails does not stop the campaign. Some of them are expected to
# fail: a window sized at one BDP on a lossy link may not finish the workload
# inside the timeout, and that is a result rather than an accident. Failures
# are recorded and the campaign moves on.
#
# Usage:
#   KUBECONFIG=bench.kubeconfig ./run.sh runs
#
# Env:
#   RAMMUX_PERF  path to the binary          (default: rammux-perf on PATH)
#   RESULTS      where logs and summaries go (default: results)
#   RETRIES      attempts per config         (default: 2)

set -uo pipefail

RUNS_DIR="${1:-runs}"
RESULTS="${RESULTS:-results}"
RAMMUX_PERF="${RAMMUX_PERF:-rammux-perf}"
RETRIES="${RETRIES:-2}"

if [[ ! -f "$RUNS_DIR/plan.txt" ]]; then
    echo "no plan at $RUNS_DIR/plan.txt - run gen.py first" >&2
    exit 1
fi
if [[ -z "${KUBECONFIG:-}" ]]; then
    echo "KUBECONFIG is not set" >&2
    exit 1
fi

mkdir -p "$RESULTS/logs" "$RESULTS/summaries" "$RESULTS/driver"
export RUST_LOG="${RUST_LOG:-info}"

total=$(grep -c . "$RUNS_DIR/plan.txt")
index=0
failed=()

# The configs name their log as results/logs/<name>.log, relative to the
# working directory, so the campaign has to run from the directory that holds
# `results`. Checked here rather than discovered three hours in.
if [[ "$RESULTS" != "results" ]]; then
    echo "note: configs write logs to results/logs; RESULTS=$RESULTS only moves the summaries" >&2
fi

while read -r name; do
    [[ -n "$name" ]] || continue
    index=$((index + 1))
    config="$RUNS_DIR/$name.json"
    log="results/logs/$name.log"
    summary="$RESULTS/summaries/$name.txt"

    if [[ -s "$log" ]]; then
        printf '[%3d/%3d] %-44s skip (already have its log)\n' "$index" "$total" "$name"
        continue
    fi

    for attempt in $(seq 1 "$RETRIES"); do
        printf '[%3d/%3d] %-44s attempt %d ... ' "$index" "$total" "$name" "$attempt"
        started=$SECONDS
        # stdout is the summary table, stderr is the run's own tracing. Both
        # are kept: the tracing is where a failure says what went wrong.
        if "$RAMMUX_PERF" k8s run --json-log --config-path "$config" \
                > "$summary" 2> "$RESULTS/driver/$name.stderr"; then
            printf 'ok (%ds)\n' "$((SECONDS - started))"
            break
        fi
        printf 'FAILED (%ds)\n' "$((SECONDS - started))"
        tail -n 2 "$RESULTS/driver/$name.stderr" | sed 's/^/           /'
        if [[ "$attempt" == "$RETRIES" ]]; then
            failed+=("$name")
        else
            # Give the cluster a moment: a failure is often a namespace still
            # terminating, or an API server that was briefly unhappy.
            sleep 15
        fi
    done
done < "$RUNS_DIR/plan.txt"

echo
echo "done: $((total - ${#failed[@]}))/$total runs produced a log"
if (( ${#failed[@]} )); then
    echo "failed:"
    printf '  %s\n' "${failed[@]}"
fi
