#!/bin/bash

# Load the POC datasets into Datalayers.
# Usage:
#   ./poc/scripts/load_data_poc.sh          # load the fresh POC data
#   ./poc/scripts/load_data_poc.sh stale    # load the stale POC data
#
# 数据文件路径按 scale（POC_SCALE / POC_STALE_SCALE）自动推导，与
# gen_data_poc.sh 的产物文件名保持一致；也可用 DATA_FILE 显式指定。
#
# Env:
#   SQL_ENDPOINT  Arrow Flight SQL 地址（覆盖 load_config yaml 中的 sql-endpoint）
#   POC_SCALE / POC_STALE_SCALE  主机数（与 gen_data_poc.sh 保持一致）
#   DATA_FILE     显式指定数据文件路径
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
    SCALE="${POC_SCALE:-1000000}"
    START_TIMESTAMP="2026-01-01T00:00:00Z"
    ;;
  stale)
    CONFIG_FILE="load_stale_data_poc.yaml"
    SCALE="${POC_STALE_SCALE:-100000}"
    START_TIMESTAMP="2025-12-31T00:00:00Z"
    ;;
  *)
    echo "Usage: $0 [stale]"
    echo "  (no arg): load via poc/load_config/load_data_poc.yaml"
    echo "  stale   : load via poc/load_config/load_stale_data_poc.yaml"
    exit 1
    ;;
esac

DATA_FILE="${DATA_FILE:-./generated_data/datalayers/cpu-only-${SCALE}-$(date -u -d "${START_TIMESTAMP}" +%Y%m%d)-12h.data}"

echo "Loading via ./poc/load_config/${CONFIG_FILE} ..."
echo "Data file: ${DATA_FILE}"

ARGS=()
if [ -n "${SQL_ENDPOINT:-}" ]; then
  ARGS+=(--loader.db-specific.sql-endpoint="${SQL_ENDPOINT}")
fi
ARGS+=(--data-source.file.location="${DATA_FILE}")

# 额外的 loader 覆盖参数（空格分隔的 --key=value 列表）。
# 供稳定性测试覆盖目标库/并发等，例如：
#   LOAD_EXTRA_ARGS="--loader.runner.db-name=benchmark_stable --loader.runner.workers=8"
# shellcheck disable=SC2206
if [ -n "${LOAD_EXTRA_ARGS:-}" ]; then
  ARGS+=(${LOAD_EXTRA_ARGS})
fi

./bin/tsbs_load load datalayers --config=./poc/load_config/${CONFIG_FILE} "${ARGS[@]}"
