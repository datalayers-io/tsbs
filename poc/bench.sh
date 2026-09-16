#!/usr/bin/env bash
#
# bench.sh - Datalayers TSBS POC 一键压测入口。
#
# 读取同目录下的 bench_config.yaml，按配置依次执行：
#   建库建表(create_db_table) -> 生成数据(gen_data) -> 生成查询(gen_queries)
#   -> 灌数(load_data) -> 跑全部查询(run_queries)
# 并在开始前探测：datalayers HTTP/Flight 端口是否可通、dlsql 是否可用、
# tsbs 二进制是否齐全。
#
# 用法：
#   ./poc/bench.sh [config.yaml] [smoke]
#   ./poc/bench.sh smoke
#   - 不传参数则使用 ./poc/bench_config.yaml
#   - 带 smoke 时进入小规模冒烟模式：POC_SCALE=1000、POC_STALE_SCALE=100
#     （数据量约 fresh 168MB / stale 17MB），快速验证全流程
#
# 结果输出：./results/poc-<时间戳>/（load 日志 + 查询汇总表格）
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

SMOKE=0
case "${1:-}" in
  smoke)
    SMOKE=1
    CONFIG="${SCRIPT_DIR}/bench_config.yaml"
    ;;
  *)
    CONFIG="${1:-${SCRIPT_DIR}/bench_config.yaml}"
    if [ "${2:-}" = "smoke" ]; then SMOKE=1; fi
    ;;
esac
[ -f "${CONFIG}" ] || { echo "ERROR: 配置文件不存在: ${CONFIG}" >&2; exit 1; }

# ── 读取配置（扁平 key: value 的 yaml）───────────────────────────────────
get_cfg() {
  local key="$1" default="$2" v
  v="$(grep -E "^[[:space:]]*${key}[[:space:]]*:" "${CONFIG}" | head -1 \
        | sed -E 's/^[[:space:]]*[^:]*:[[:space:]]*//; s/[[:space:]]+$//' \
        | sed -E 's/^"(.*)"$/\1/' )"
  [ -n "${v}" ] && echo "${v}" || echo "${default}"
}

FLIGHT_ADDR="$(get_cfg flight_addr "localhost:8360")"
HTTP_ADDR="$(get_cfg http_addr "localhost:8361")"
DLSQL_DIR="$(get_cfg dlsql_dir "")"
DLSQL_EXTRA_ARGS="$(get_cfg dlsql_extra_args "")"
DLSQL_TIMEOUT="$(get_cfg dlsql_timeout "300")"
CREATE_DB_TABLE="$(get_cfg create_db_table "true")"
GEN_DATA="$(get_cfg gen_data "true")"
GEN_QUERIES="$(get_cfg gen_queries "true")"
LOAD_DATA="$(get_cfg load_data "true")"
RUN_QUERIES="$(get_cfg run_queries "true")"
QUERY_WORKERS="$(get_cfg query_workers "64")"

is_true() { [ "${1}" = "true" ]; }

# est_size 估算数据文件大小：每台主机 12h/30s = 1440 行，约 122B/行。
est_size() {
  awk -v s="$1" 'BEGIN{ b=s*1440*122; if(b>=1073741824) printf "~%.1fGB", b/1073741824; else printf "~%.0fMB", b/1048576 }'
}

# smoke 冒烟模式：强制小规模（子脚本通过环境变量 POC_SCALE/POC_STALE_SCALE 感知）。
if [ "${SMOKE}" -eq 1 ]; then
  export POC_SCALE=1000
  export POC_STALE_SCALE=100
  echo ">>> SMOKE 冒烟模式：fresh scale=${POC_SCALE}（约 $(est_size 1000) 数据, 1440000 行）, "
  echo "    stale scale=${POC_STALE_SCALE}（约 $(est_size 100) 数据, 144000 行）"
fi
FRESH_SCALE="${POC_SCALE:-1000000}"
STALE_SCALE="${POC_STALE_SCALE:-100000}"

RESULTS_DIR="./results/poc-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${RESULTS_DIR}"
LOAD_LOG="${RESULTS_DIR}/load.log"

# ── 探测 ─────────────────────────────────────────────────────────────────
tcp_reachable() { # tcp_reachable <host> <port>
  local host="$1" port="$2"
  timeout 3 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null
}

HTTP_HOST="${HTTP_ADDR%:*}"; HTTP_PORT="${HTTP_ADDR##*:}"
FLIGHT_HOST="${FLIGHT_ADDR%:*}"; FLIGHT_PORT="${FLIGHT_ADDR##*:}"

echo "==> 探测 Datalayers HTTP 端口 ${HTTP_ADDR} ..."
tcp_reachable "${HTTP_HOST}" "${HTTP_PORT}" \
  || { echo "ERROR: Datalayers HTTP 端口不可达: ${HTTP_ADDR}" >&2; exit 1; }
echo "OK  HTTP 端口可通: ${HTTP_ADDR}"

echo "==> 探测 Datalayers Flight SQL 端口 ${FLIGHT_ADDR} ..."
tcp_reachable "${FLIGHT_HOST}" "${FLIGHT_PORT}" \
  || { echo "ERROR: Datalayers Flight SQL 端口不可达: ${FLIGHT_ADDR}" >&2; exit 1; }
echo "OK  Flight 端口可通: ${FLIGHT_ADDR}"

if [ -n "${DLSQL_DIR}" ]; then
  DLSQL_BIN="${DLSQL_DIR}/dlsql"
else
  DLSQL_BIN="$(command -v dlsql || true)"
fi
echo "==> 探测 dlsql ..."
[ -n "${DLSQL_BIN}" ] && [ -x "${DLSQL_BIN}" ] \
  || { echo "ERROR: dlsql 不可用（DLSQL_DIR 未配置或不在 PATH）: ${DLSQL_BIN}" >&2; exit 1; }
echo "OK  dlsql: ${DLSQL_BIN}"

echo "==> 探测 dlsql 连通性（10s 超时）..."
timeout 10 "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" -e "select 1" >/dev/null 2>&1 \
  || { echo "ERROR: dlsql 无法连接 Datalayers ${FLIGHT_ADDR}" >&2; exit 1; }
echo "OK  dlsql 可连接: ${FLIGHT_ADDR}"

echo "==> 探测 tsbs 二进制 ..."
for b in tsbs_generate_data tsbs_generate_queries tsbs_load \
         tsbs_run_queries_datalayers rewrite_query_hints_config; do
  [ -x "./bin/${b}" ] || { echo "ERROR: 缺少二进制 bin/${b}（先运行 make all）" >&2; exit 1; }
done
echo "OK  tsbs 二进制齐全"

# ── 建库建表 ─────────────────────────────────────────────────────────────
if is_true "${CREATE_DB_TABLE}"; then
  echo ""
  echo "==> create_db_table: 用 dlsql 执行 ${SCRIPT_DIR}/sql/create.sql（超时 ${DLSQL_TIMEOUT}s）"
  # dlsql 连接的是 Arrow Flight SQL 端口（flight_addr），而非 HTTP 端口。
  # dlsql 无内置超时，这里用 timeout 包裹避免服务端 DDL 卡住时脚本无限等待。
  # shellcheck disable=SC2086
  timeout "${DLSQL_TIMEOUT}s" "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" \
    ${DLSQL_EXTRA_ARGS} --load-file "${SCRIPT_DIR}/sql/create.sql" \
    || { echo "ERROR: 执行 create.sql 失败或超时（${DLSQL_TIMEOUT}s）。可调大 dlsql_timeout 后重试。" >&2; exit 1; }
  echo "OK  建库建表完成"
fi

# ── 生成数据 ─────────────────────────────────────────────────────────────
if is_true "${GEN_DATA}"; then
  echo ""
  echo "==> gen_data: 生成 fresh 数据（scale=${FRESH_SCALE}, 约 $(est_size "${FRESH_SCALE}")）"
  ./poc/scripts/gen_data_poc.sh
  echo "==> gen_data: 生成 stale 数据（scale=${STALE_SCALE}, 约 $(est_size "${STALE_SCALE}")）"
  ./poc/scripts/gen_data_poc.sh stale
fi

# ── 生成查询 ─────────────────────────────────────────────────────────────
if is_true "${GEN_QUERIES}"; then
  echo ""
  echo "==> gen_queries: 生成全部 15 种查询"
  ./poc/scripts/gen_queries_poc.sh
  echo "==> rewrite_queries: 按 poc_hints.yaml 重写 SQL hint"
  ./poc/scripts/rewrite_queries_poc.sh
fi

# ── 灌数 ─────────────────────────────────────────────────────────────────
load_rows_rate() { # load_rows_rate <log> -> rows/sec（tsbs_load 摘要行）
  grep -oE "loaded [0-9]+ rows in [0-9.]+sec with [0-9]+ workers \(mean rate [0-9.]+ rows/sec\)" \
    "${1}" | head -1
}

if is_true "${LOAD_DATA}"; then
  echo ""
  echo "==> load_data: 灌入 fresh 数据"
  SQL_ENDPOINT="${FLIGHT_ADDR}" ./poc/scripts/load_data_poc.sh | tee "${LOAD_LOG}.fresh"

  echo ""
  echo "==> load_data: 灌入 stale 数据"
  SQL_ENDPOINT="${FLIGHT_ADDR}" ./poc/scripts/load_data_poc.sh stale | tee "${LOAD_LOG}.stale"
fi

# ── 跑全部查询 ───────────────────────────────────────────────────────────
if is_true "${RUN_QUERIES}"; then
  echo ""
  echo "==> run_queries: 跑全部 15 种查询（workers=${QUERY_WORKERS}）"
  SQL_ENDPOINT="${FLIGHT_ADDR}" RESULTS_DIR="${RESULTS_DIR}" \
    ./poc/scripts/run_all_queries_poc.sh "${QUERY_WORKERS}"
fi

# ── 汇总指标 ─────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "POC 结果汇总（输出目录: ${RESULTS_DIR}"
echo "================================================================"

if is_true "${LOAD_DATA}"; then
  echo ""
  echo "--- Load 指标 ---"
  if [ -f "${LOAD_LOG}.fresh" ]; then
    r1="$(load_rows_rate "${LOAD_LOG}.fresh")"
    [ -n "${r1}" ] && echo "fresh: ${r1}" || echo "fresh: (未解析到 tsbs_load 摘要)"
  fi
  if [ -f "${LOAD_LOG}.stale" ]; then
    r2="$(load_rows_rate "${LOAD_LOG}.stale")"
    [ -n "${r2}" ] && echo "stale: ${r2}" || echo "stale: (未解析到 tsbs_load 摘要)"
  fi
  # 注：tsbs_load 原生不提供单条写入延迟指标，只提供吞吐（rows/sec / metrics/sec）；
  #     详细的每秒进度在 load.log.fresh / load.log.stale 中（reporting-period）。
fi

if is_true "${RUN_QUERIES}"; then
  echo ""
  echo "--- 查询汇总（见上方 run_all_queries_poc.sh 输出的表格）---"
fi

echo ""
echo "done. 结果目录: ${RESULTS_DIR}"
