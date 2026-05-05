#!/usr/bin/env bash
#
# C2 fanout reproducer: 1 publisher + N subscribers, single Aulon server,
# publish-to-deliver latency histogram aggregated across subscribers.
#
# Pins aulon-server to one CPU and aulon-fanout (which hosts the publisher
# and all N subscribers in one tokio_uring runtime) to a different CPU so
# server-side and client-side overheads do not contend. Both binaries are
# built in release.
#
# Environment overrides:
#   AULON_SERVER_CPU      (default 0)
#   AULON_CLIENT_CPU      (default 1)
#   AULON_FANOUT          (default 8) — number of subscribers
#   AULON_ITERATIONS      (default 50000)
#   AULON_WARMUP          (default 1000)
#   AULON_PAYLOAD_BYTES   (default 256, must be >= 8)
#   AULON_ADDR            (default 127.0.0.1:4222)
#   CARGO_TARGET_DIR      (default ./target)
#
# Linux only. Requires `taskset` (util-linux).

set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "bench/fanout.sh: Linux only (need taskset and io_uring)" >&2
    exit 2
fi

if ! command -v taskset >/dev/null 2>&1; then
    echo "bench/fanout.sh: 'taskset' not found; install util-linux" >&2
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
cargo build --release -p aulon-server -p aulon-bench --bin aulon-fanout --bin aulon-server

SERVER_BIN="$TARGET_DIR/release/aulon-server"
CLIENT_BIN="$TARGET_DIR/release/aulon-fanout"

if [[ ! -x "$SERVER_BIN" || ! -x "$CLIENT_BIN" ]]; then
    echo "bench/fanout.sh: missing binaries under $TARGET_DIR/release" >&2
    exit 1
fi

echo "==> Pinning server to CPU $SERVER_CPU"
taskset -c "$SERVER_CPU" "$SERVER_BIN" &
SERVER_PID=$!

cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Give the server a moment to bind.
sleep 0.3

echo "==> Running fanout client on CPU $CLIENT_CPU (subscribers=$AULON_FANOUT)"
taskset -c "$CLIENT_CPU" "$CLIENT_BIN"
