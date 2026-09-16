#!/bin/bash

# Generate all 15 POC query types for Datalayers, aligned with the query
# numbering used by run_queries_poc.sh. Queries target the POC dataset
# (scale 1,000,000 hosts, 2026-01-01 00:00 ~ 12:00) and land under
# ./generated_query/datalayers/cpu-only/poc/.
#
# Usage: ./poc/scripts/gen_queries_poc.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

FORMAT="datalayers"
USE_CASE="cpu-only"
SCALE=1000000
START_TIMESTAMP="2026-01-01T00:00:00Z"
END_TIMESTAMP="2026-01-01T12:00:01Z"
NUM_QUERIES="${NUM_QUERIES:-1000}"
SEED=42

QUERY_TYPES=(
  "single-groupby-1-1-1"
  "single-groupby-1-1-12"
  "single-groupby-1-8-1"
  "single-groupby-5-1-1"
  "single-groupby-5-1-12"
  "single-groupby-5-8-1"
  "cpu-max-all-1"
  "cpu-max-all-8"
  "double-groupby-1"
  "double-groupby-5"
  "double-groupby-all"
  "high-cpu-1"
  "high-cpu-all"
  "lastpoint"
  "groupby-orderby-limit"
)

for QUERY_TYPE in "${QUERY_TYPES[@]}"; do
  OUT="./generated_query/${FORMAT}/${USE_CASE}/poc/${QUERY_TYPE}.query"

  mkdir -p "$(dirname "$OUT")"

  echo "Generating ${OUT} ..."

  ./bin/tsbs_generate_queries \
    --format="$FORMAT" \
    --use-case="$USE_CASE" \
    --query-type="$QUERY_TYPE" \
    --queries=$NUM_QUERIES \
    --scale=$SCALE \
    --seed=$SEED \
    --timestamp-start="$START_TIMESTAMP" \
    --timestamp-end="$END_TIMESTAMP" \
    --file="$OUT"
done

echo "Done. Queries are under ./generated_query/datalayers/cpu-only/poc/"
