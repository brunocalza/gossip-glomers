#!/usr/bin/env bash
set -euo pipefail

MAELSTROM=~/projects/maelstrom/maelstrom
BIN=~/go/bin/fault-tolerant-efficient-2
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STORE_DIR="$SCRIPT_DIR/store"

RETRIES=(100 200 300 500)
BRANCHES=(1 2 4 8 16)

# Build
echo "Building..."
(cd "$(dirname "$0")" && go install)

# Print CSV header
echo "retry_ms,branch,msgs_per_op,p50,p95,p99"

for retry in "${RETRIES[@]}"; do
  for branch in "${BRANCHES[@]}"; do
    echo "--- Running retry=${retry}ms branch=${branch} ---" >&2

    RETRY=$retry BRANCH=$branch $MAELSTROM test -w broadcast \
      --bin "$BIN" \
      --node-count 25 \
      --time-limit 20 \
      --rate 100 \
      --latency 100 \
      >/dev/null 2>&1 || true

    results="$STORE_DIR/latest/results.edn"
    if [[ ! -f "$results" ]]; then
      echo "${retry},${branch},ERR,ERR,ERR,ERR"
      continue
    fi

    flat=$(tr '\n' ' ' < "$results")
    msgs_per_op=$(echo "$flat" | grep -oP ':msgs-per-op\s+\K[0-9.]+' | head -1)
    p50=$(echo "$flat" | grep -oP ':stable-latencies\s*\{[^}]*0\.5\s+\K[0-9]+' | head -1)
    p95=$(echo "$flat" | grep -oP ':stable-latencies\s*\{[^}]*0\.95\s+\K[0-9]+' | head -1)
    p99=$(echo "$flat" | grep -oP ':stable-latencies\s*\{[^}]*0\.99\s+\K[0-9]+' | head -1)

    echo "${retry},${branch},${msgs_per_op:-?},${p50:-?},${p95:-?},${p99:-?}"
  done
done
