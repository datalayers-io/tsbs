#!/bin/bash

# Load the POC datasets into Datalayers.
# Usage:
#   ./poc/scripts/load_data_poc.sh          # load the fresh POC data
#   ./poc/scripts/load_data_poc.sh stale    # load the stale POC data
#
# Env:
#   SQL_ENDPOINT  Arrow Flight SQL 地址（覆盖 load_config yaml 中的 sql-endpoint）
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

unset http_proxy https_proxy

MODE="${1:-}"
case "${MODE}" in
  "")
    CONFIG_FILE="load_data_poc.yaml"
    ;;
  stale)
    CONFIG_FILE="load_stale_data_poc.yaml"
    ;;
  *)
    echo "Usage: $0 [stale]"
    echo "  (no arg): load via poc/load_config/load_data_poc.yaml"
    echo "  stale   : load via poc/load_config/load_stale_data_poc.yaml"
    exit 1
    ;;
esac

echo "Loading via ./poc/load_config/${CONFIG_FILE} ..."

ARGS=()
if [ -n "${SQL_ENDPOINT:-}" ]; then
  ARGS+=(--loader.db-specific.sql-endpoint="${SQL_ENDPOINT}")
fi

./bin/tsbs_load load datalayers --config=./poc/load_config/${CONFIG_FILE} "${ARGS[@]}"
