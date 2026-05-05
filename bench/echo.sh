#!/usr/bin/env bash
#
# C1 headline reproducer: single-core 256B TCP echo, RTT histogram.
#
# Pins aulon-server to one CPU and aulon-bench to a different CPU so the
# server-side and client-side overheads do not contend. Both binaries are
# built in release.
#
# Environment overrides:
#   AULON_SERVER_CPU      (default 0)
#   AULON_CLIENT_CPU      (default 1)
#   AULON_ITERATIONS      (default 100000)
#   AULON_WARMUP          (default 1000)
#   AULON_PAYLOAD_BYTES   (default 256)
#   AULON_ADDR            (default 127.0.0.1:4222)
#   CARGO_TARGET_DIR      (default ./target)
#
# Linux only. Requires `taskset` (util-linux).

set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "bench/echo.sh: Linux only (need taskset and io_uring)" >&2
    exit 2
fi

if ! command -v taskset >/dev/null 2>&1; then
    echo "bench/echo.sh: 'taskset' not found; install util-linux" >&2
    exit 2
fi

SERVER_CPU="${AULON_SERVER_CPU:-0}"
CLIENT_CPU="${AULON_CLIENT_CPU:-1}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"

export AULON_ITERATIONS="${AULON_ITERATIONS:-100000}"
export AULON_WARMUP="${AULON_WARMUP:-1000}"
export AULON_PAYLOAD_BYTES="${AULON_PAYLOAD_BYTES:-256}"
export AULON_ADDR="${AULON_ADDR:-127.0.0.1:4222}"

echo "==> Building release binaries"
cargo build --release -p aulon-server -p aulon-bench

SERVER_BIN="$TARGET_DIR/release/aulon-server"
CLIENT_BIN="$TARGET_DIR/release/aulon-bench"

if [[ ! -x "$SERVER_BIN" || ! -x "$CLIENT_BIN" ]]; then
    echo "bench/echo.sh: missing binaries under $TARGET_DIR/release" >&2
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

echo "==> Running client on CPU $CLIENT_CPU"
taskset -c "$CLIENT_CPU" "$CLIENT_BIN"
