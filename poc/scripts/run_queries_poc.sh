#!/bin/bash

# Run a POC query against Datalayers.
# Usage: ./poc/scripts/run_queries_poc.sh <workers> <query-number>
#   query-number: 1~15, same mapping as run_queries_datalayers.sh
#
# The query files live under generated_query/datalayers/cpu-only/poc/ and are
# generated for the POC scale (1,000,000 hosts) and 2026-01-01 timestamps.
#
# Env:
#   SQL_ENDPOINT  Arrow Flight SQL 地址（默认 localhost:8360）
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

SQL_ENDPOINT="${SQL_ENDPOINT:-localhost:8360}"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <workers> <query-number (1~15)>"
  exit 1
fi

WORKERS="$1"
QUERY_ID="$2"

case "$QUERY_ID" in
  1)  QUERY_TYPE="single-groupby-1-1-1" ;;
  2)  QUERY_TYPE="single-groupby-1-1-12" ;;
  3)  QUERY_TYPE="single-groupby-1-8-1" ;;
  4)  QUERY_TYPE="single-groupby-5-1-1" ;;
  5)  QUERY_TYPE="single-groupby-5-1-12" ;;
  6)  QUERY_TYPE="single-groupby-5-8-1" ;;
  7)  QUERY_TYPE="cpu-max-all-1" ;;
  8)  QUERY_TYPE="cpu-max-all-8" ;;
  9)  QUERY_TYPE="double-groupby-1" ;;
  10) QUERY_TYPE="double-groupby-5" ;;
  11) QUERY_TYPE="double-groupby-all" ;;
  12) QUERY_TYPE="high-cpu-1" ;;
  13) QUERY_TYPE="high-cpu-all" ;;
  14) QUERY_TYPE="lastpoint" ;;
  15) QUERY_TYPE="groupby-orderby-limit" ;;
  *)
    echo "Invalid QUERY_ID: ${QUERY_ID}"
    echo "QUERY_ID options: 1 ~ 15"
    exit 1
    ;;
esac

unset http_proxy https_proxy

QUERY_FILE="./generated_query/datalayers/cpu-only/poc/${QUERY_TYPE}.query"

./bin/tsbs_run_queries_datalayers \
    --file="$QUERY_FILE" \
    --sql-endpoint="${SQL_ENDPOINT}" \
    --workers=$WORKERS \
    --burn-in=10
