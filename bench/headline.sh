#!/usr/bin/env bash
#
# C4 headline reproducer: aulon-fanout drives the same workload
# against `aulon-server` and against `nats-server` back-to-back, and
# prints a comparison of publish-to-deliver latencies.
#
# The workload is identical for both backends: 1 publisher + N
# subscribers on the same NATS-core wire protocol, all hosted in one
# tokio-uring-driven aulon-fanout binary on a different CPU from the
# server.
#
# Environment overrides:
#   AULON_SERVER_CPU      (default 0)
#   AULON_CLIENT_CPU      (default 1)
#   AULON_FANOUT          (default 8) — number of subscribers
#   AULON_ITERATIONS      (default 50000)
#   AULON_WARMUP          (default 1000)
#   AULON_PAYLOAD_BYTES   (default 256, must be >= 8)
#   AULON_ADDR            (default 127.0.0.1:4222)
#   AULON_FORCE_SHARDS    (passthrough; default unset = topology detect)
#   CARGO_TARGET_DIR      (default ./target)
#   NATS_SERVER_BIN       (default `nats-server` on PATH)
#
# Linux only. Requires `taskset` (util-linux) and the official
# `nats-server` binary on PATH (or via NATS_SERVER_BIN).

set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "bench/headline.sh: Linux only (need taskset and io_uring)" >&2
    exit 2
fi

if ! command -v taskset >/dev/null 2>&1; then
    echo "bench/headline.sh: 'taskset' not found; install util-linux" >&2
    exit 2
fi

NATS_SERVER_BIN="${NATS_SERVER_BIN:-nats-server}"
if ! command -v "$NATS_SERVER_BIN" >/dev/null 2>&1; then
    echo "bench/headline.sh: '$NATS_SERVER_BIN' not found on PATH" >&2
    echo "  install via: sudo apt install nats-server" >&2
    exit 2
fi

SERVER_CPU="${AULON_SERVER_CPU:-0}"
CLIENT_CPU="${AULON_CLIENT_CPU:-1}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"

export AULON_FANOUT="${AULON_FANOUT:-8}"
export AULON_ITERATIONS="${AULON_ITERATIONS:-50000}"
export AULON_WARMUP="${AULON_WARMUP:-1000}"
export AULON_PAYLOAD_BYTES="${AULON_PAYLOAD_BYTES:-256}"
export AULON_ADDR="${AULON_ADDR:-127.0.0.1:4222}"

echo "==> Building release binaries"
cargo build --release -p aulon-server -p aulon-bench --bin aulon-fanout --bin aulon-server >/dev/null

AULON_BIN="$TARGET_DIR/release/aulon-server"
CLIENT_BIN="$TARGET_DIR/release/aulon-fanout"

if [[ ! -x "$AULON_BIN" || ! -x "$CLIENT_BIN" ]]; then
    echo "bench/headline.sh: missing binaries under $TARGET_DIR/release" >&2
    exit 1
fi

run_one() {
    local label="$1"; shift
    local server_cmd=("$@")

    echo
    echo "==> ${label}: server pinned to CPU $SERVER_CPU"
    "${server_cmd[@]}" >/tmp/headline-${label}.srvlog 2>&1 &
    local server_pid=$!
    sleep 0.5

    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "bench/headline.sh: ${label} server failed to start; log:" >&2
        cat /tmp/headline-${label}.srvlog >&2
        return 1
    fi

    echo "==> ${label}: client pinned to CPU $CLIENT_CPU (subscribers=$AULON_FANOUT, iterations=$AULON_ITERATIONS)"
    if ! taskset -c "$CLIENT_CPU" "$CLIENT_BIN" >/tmp/headline-${label}.cliout 2>&1; then
        echo "bench/headline.sh: ${label} client failed; log:" >&2
        tail -40 /tmp/headline-${label}.cliout >&2
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
        return 1
    fi

    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true

    cat /tmp/headline-${label}.cliout
}

extract_metric() {
    local file="$1" metric="$2"
    awk -v m="$metric ns" '
        index($0, m) == 1 && index($0, ":") {
            n = index($0, ":"); v = substr($0, n + 1); gsub(/^[ \t]+|[ \t]+$/, "", v);
            print v; exit
        }
        # Some entries are indented; strip leading whitespace and retry.
        {
            s = $0; sub(/^[ \t]+/, "", s);
            if (index(s, m) == 1 && index(s, ":")) {
                n = index(s, ":"); v = substr(s, n + 1); gsub(/^[ \t]+|[ \t]+$/, "", v);
                print v; exit
            }
        }
    ' "$file"
}

run_one "aulon" taskset -c "$SERVER_CPU" "$AULON_BIN"

# nats-server: -DV is verbose+debug; we want quiet, but flush logs on
# crash. -m 0 keeps the monitoring port off.
run_one "nats" taskset -c "$SERVER_CPU" "$NATS_SERVER_BIN" -a 127.0.0.1 -p 4222 -l /dev/null

echo
echo "==================================================================="
echo "  C4 headline: ${AULON_FANOUT}x subscribers, ${AULON_PAYLOAD_BYTES} B payload, ${AULON_ITERATIONS} iters"
echo "==================================================================="
printf "%-12s %-12s %-12s %-12s %-12s\n" "backend" "min ns" "p50 ns" "p99 ns" "p99.99 ns"
for label in aulon nats; do
    f=/tmp/headline-${label}.cliout
    min=$(extract_metric "$f" "min")
    p50=$(extract_metric "$f" "p50")
    p99=$(extract_metric "$f" "p99")
    p99_99=$(extract_metric "$f" "p99.99")
    printf "%-12s %-12s %-12s %-12s %-12s\n" "$label" "$min" "$p50" "$p99" "$p99_99"
done
echo "==================================================================="
echo "Logs:"
echo "  aulon: /tmp/headline-aulon.srvlog  /tmp/headline-aulon.cliout"
echo "  nats:  /tmp/headline-nats.srvlog   /tmp/headline-nats.cliout"
