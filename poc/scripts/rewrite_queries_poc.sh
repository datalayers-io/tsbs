#!/bin/bash

# Rewrite the SQL hints (parallel_degree / skip_rollup) of all POC query files
# according to a per-query YAML config. Rewrites the whole poc query dir in
# place.
#
# Usage:
#   ./poc/scripts/rewrite_queries_poc.sh [config.yaml] [query-dir]
#
#   config.yaml (default: ./poc/scripts/poc_hints.yaml)
#   query-dir  (default: ./generated_query/datalayers/cpu-only/poc)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

CONFIG="${1:-./poc/scripts/poc_hints.yaml}"
QUERY_DIR="${2:-./generated_query/datalayers/cpu-only/poc}"

if [ ! -f "${CONFIG}" ]; then
  echo "ERROR: config file not found: ${CONFIG}" >&2
  exit 1
fi
if [ ! -d "${QUERY_DIR}" ]; then
  echo "ERROR: query dir not found: ${QUERY_DIR} (run gen_queries_poc.sh first)" >&2
  exit 1
fi

./bin/rewrite_query_hints_config "${CONFIG}" "${QUERY_DIR}"
