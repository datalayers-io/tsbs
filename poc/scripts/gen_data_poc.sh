#!/bin/bash

# Generate the POC datasets for Datalayers.
# Usage:
#   ./poc/scripts/gen_data_poc.sh          # fresh data
#   ./poc/scripts/gen_data_poc.sh stale    # stale (out-of-date) data
#
# Fresh data: 1,000,000 hosts (--scale), one point every 30s (--log-interval),
# covering 2026-01-01 00:00:00 ~ 2026-01-01 12:00:00 UTC.
# Total points = 1,000,000 * (12h / 30s) = 1.44e9.
#
# Stale data: same log-interval (30s) and window length (12h), but all
# timestamps BEFORE the fresh dataset, covering 2025-12-31 00:00:00 ~ 12:00:00
# UTC. Scale is 100,000 hosts, i.e. 10% of the fresh dataset's 1,000,000 hosts,
# so the stale dataset is ~10% of the fresh one by total points.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

FORMAT="datalayers"
USE_CASE="cpu-only"
LOG_INTERVAL="30s"
SEED=42

MODE="${1:-}"
case "${MODE}" in
  "")
    SCALE=1000000
    START_TIMESTAMP="2026-01-01T00:00:00Z"
    END_TIMESTAMP="2026-01-01T12:00:00Z"
    ;;
  stale)
    SCALE=100000
    START_TIMESTAMP="2025-12-31T00:00:00Z"
    END_TIMESTAMP="2025-12-31T12:00:00Z"
    ;;
  *)
    echo "Usage: $0 [stale]"
    echo "  (no arg): generate the fresh POC data (2026-01-01)"
    echo "  stale   : generate the stale/out-of-date POC data (2025-12-31)"
    exit 1
    ;;
esac

DATE=$(date -u -d "${START_TIMESTAMP}" +%Y%m%d)
OUT="./generated_data/${FORMAT}/cpu-only-${SCALE}-${DATE}-12h.data"

mkdir -p "$(dirname "$OUT")"

echo "Generating ${OUT} ..."

./bin/tsbs_generate_data \
    --format="$FORMAT" \
    --use-case="$USE_CASE" \
    --scale=$SCALE \
    --log-interval="$LOG_INTERVAL" \
    --seed=$SEED \
    --timestamp-start="$START_TIMESTAMP" \
    --timestamp-end="$END_TIMESTAMP" \
    --file="$OUT"
