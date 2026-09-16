#!/usr/bin/env bash
# bench_common.sh - 供 poc/scripts/*.sh 复用的公共函数：
#   - 读取 poc/bench_config.yaml（可用 BENCH_CONFIG 环境变量覆盖路径）
#   - 探测：datalayers HTTP/Flight 端口、dlsql、dldump
#
# 用法（在脚本开头 source）：
#   SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
#   REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
#   . "${SCRIPT_DIR}/bench_common.sh"
#
# 提供的变量（load_bench_config 后可用）：
#   FLIGHT_ADDR / HTTP_ADDR / FLIGHT_HOST / FLIGHT_PORT / HTTP_HOST / HTTP_PORT
#   DLSQL_DIR / DLSQL_BIN / DLSQL_TIMEOUT
#   DLDUMP_DIR / DLDUMP_BIN / DLDUMP_TIMEOUT
#   DATABASE

BENCH_CONFIG="${BENCH_CONFIG:-${REPO_DIR}/poc/bench_config.yaml}"

info() { echo "==> $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

# get_cfg <key> <default>：读取扁平 key: value 的 yaml 配置
get_cfg() {
  local key="$1" default="$2" v
  v="$(grep -E "^[[:space:]]*${key}[[:space:]]*:" "${BENCH_CONFIG}" | head -1 \
        | sed -E 's/^[[:space:]]*[^:]*:[[:space:]]*//; s/[[:space:]]+$//' \
        | sed -E 's/^"(.*)"$/\1/')"
  [ -n "${v}" ] && echo "${v}" || echo "${default}"
}

# load_bench_config：读取 bench_config.yaml 并解析各地址/工具路径
load_bench_config() {
  [ -f "${BENCH_CONFIG}" ] || die "配置文件不存在: ${BENCH_CONFIG}"

  FLIGHT_ADDR="$(get_cfg flight_addr "localhost:8360")"
  HTTP_ADDR="$(get_cfg http_addr "localhost:8361")"
  DLSQL_DIR="$(get_cfg dlsql_dir "")"
  DLDUMP_DIR="$(get_cfg dldump_dir "")"
  DLSQL_TIMEOUT="$(get_cfg dlsql_timeout "300")"
  DLDUMP_TIMEOUT="$(get_cfg dldump_timeout "600")"
  DATABASE="$(get_cfg database "benchmark")"

  HTTP_HOST="${HTTP_ADDR%:*}"; HTTP_PORT="${HTTP_ADDR##*:}"
  FLIGHT_HOST="${FLIGHT_ADDR%:*}"; FLIGHT_PORT="${FLIGHT_ADDR##*:}"

  if [ -n "${DLSQL_DIR}" ]; then
    DLSQL_BIN="${DLSQL_DIR}/dlsql"
  else
    DLSQL_BIN="$(command -v dlsql || true)"
  fi
  if [ -n "${DLDUMP_DIR}" ]; then
    DLDUMP_BIN="${DLDUMP_DIR}/dldump"
  else
    DLDUMP_BIN="$(command -v dldump || true)"
  fi
}

tcp_reachable() { # tcp_reachable <host> <port>
  local host="$1" port="$2"
  timeout 3 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null
}

# run_sql_file <sql-file>：逐条打印并执行 SQL 文件中的每条语句（用 dlsql -e）。
# 按分号切分语句、剔除 -- 注释；每条执行前打印 "==> SQL: <stmt>"。
run_sql_file() {
  local sql_file="$1" stmt
  [ -f "${sql_file}" ] || die "SQL 文件不存在: ${sql_file}"
  while IFS= read -r stmt; do
    [ -z "${stmt}" ] && continue
    echo "==> SQL: ${stmt}"
    timeout "${DLSQL_TIMEOUT}s" "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" \
      -d "${DATABASE}" -e "${stmt}" \
      || die "执行 SQL 失败: ${stmt}"
  done < <(grep -vE '^[[:space:]]*--' "${sql_file}" \
           | awk 'BEGIN{RS=";"} { gsub(/[[:space:]]+/," ",$0); gsub(/^ | $/,"",$0); if (length($0)>0) print $0 ";" }')
}

# probe_bench_env：探测 datalayers 联通性 + dlsql + dldump
probe_bench_env() {
  info "探测 Datalayers HTTP 端口 ${HTTP_ADDR} ..."
  tcp_reachable "${HTTP_HOST}" "${HTTP_PORT}" || die "HTTP 端口不可达: ${HTTP_ADDR}"
  echo "OK  HTTP 端口可通: ${HTTP_ADDR}"

  info "探测 Datalayers Flight SQL 端口 ${FLIGHT_ADDR} ..."
  tcp_reachable "${FLIGHT_HOST}" "${FLIGHT_PORT}" || die "Flight 端口不可达: ${FLIGHT_ADDR}"
  echo "OK  Flight 端口可通: ${FLIGHT_ADDR}"

  info "探测 dlsql ..."
  [ -n "${DLSQL_BIN}" ] && [ -x "${DLSQL_BIN}" ] || die "dlsql 不可用: ${DLSQL_BIN}"
  echo "OK  dlsql: ${DLSQL_BIN}"
  timeout 10 "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" -e "select 1" >/dev/null 2>&1 \
    || die "dlsql 无法连接 Datalayers ${FLIGHT_ADDR}"
  echo "OK  dlsql 可连接: ${FLIGHT_ADDR}"

  info "探测 dldump ..."
  [ -n "${DLDUMP_BIN}" ] && [ -x "${DLDUMP_BIN}" ] || die "dldump 不可用: ${DLDUMP_BIN}"
  echo "OK  dldump: ${DLDUMP_BIN}"
}
