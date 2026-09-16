#!/usr/bin/env bash
#
# test_alter.sh - 验证 Datalayers 表的加列/删列：用 dlsql --load-file
# 顺序执行 poc/sql/alter.sql（加列 tmp -> 查询 -> 删列 -> 查询）。
#
# 用法：
#   ./poc/scripts/test_alter.sh [config.yaml]
#   （默认读取 ./poc/bench_config.yaml，可通过第一个参数或 BENCH_CONFIG 覆盖）
#
# 启动时探测：datalayers HTTP/Flight 端口、dlsql、dldump（均来自 bench_config.yaml）。
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

[ -n "${1:-}" ] && BENCH_CONFIG="${1}"
# shellcheck source=bench_common.sh
. "${SCRIPT_DIR}/bench_common.sh"

load_bench_config
probe_bench_env

info "用 dlsql 逐条执行 ${REPO_DIR}/poc/sql/alter.sql（库=${DATABASE}，超时 ${DLSQL_TIMEOUT}s）"
run_sql_file "${REPO_DIR}/poc/sql/alter.sql"
echo "OK  alter 验证完成（加列/查询/删列/查询均执行成功）"
