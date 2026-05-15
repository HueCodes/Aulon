#!/usr/bin/env bash
# Regenerate the committed public-API snapshot for aulon-core.
#
# Run this from a Linux host (the workspace does not build on macOS;
# tokio-uring is Linux-only). Inside the project's OrbStack VM:
#
#     bash scripts/snapshot-public-api.sh
#
# CI invokes the same script and fails if `git diff --exit-code`
# reports drift. Every public-API change must land as a deliberate
# diff to crates/aulon-core/PUBLIC_API.txt.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SNAPSHOT="$ROOT/crates/aulon-core/PUBLIC_API.txt"

if ! command -v cargo-public-api >/dev/null 2>&1; then
    echo "cargo-public-api not installed; run: cargo install --locked cargo-public-api" >&2
    exit 2
fi

cd "$ROOT"
cargo public-api --package aulon-core --simplified > "$SNAPSHOT"
echo "wrote $SNAPSHOT"
