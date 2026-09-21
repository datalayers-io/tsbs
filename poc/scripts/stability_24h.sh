#!/usr/bin/env bash
#
# stability_24h.sh - 长时间稳定性压测驱动（基于 poc/bench.sh）。
#
# 设计：
#   - **写入独立库**（默认 benchmark_stable.cpu），**查询仍读 benchmark.cpu**，读写互不干扰。
#   - 写入、查询各自一个循环，**并发**运行；每轮 bench.sh 结束立即开始下一轮，直到满 N 小时。
#   - 测试使用**自己生成的临时 bench 配置**（落在结果目录下），不改动仓库里共享的 bench_*.yaml。
#   - 压力通过 **load workers / query workers** 控制（可选 batch/limit）。
#   - 单轮失败不中断（退避重试）；到点停发新轮次，给在跑轮次最多 --grace 秒收尾，超时按进程组强杀。
#   - Ctrl-C / SIGTERM 可安全停止。
#
# 用法：
#   ./poc/scripts/stability_24h.sh [选项]
#   短时自检： ./poc/scripts/stability_24h.sh --hours 0.17 --scale 1000 --dry-run
#   正式 24h： ./poc/scripts/stability_24h.sh --hours 24 --flight-addr $M1_IP:8360 --http-addr $M1_IP:8361 \
#                --load-workers 8 --query-workers 8
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

# ── 默认参数 ─────────────────────────────────────────────────────────────
HOURS="24"
FLIGHT_ADDR="localhost:8360"
HTTP_ADDR="localhost:8361"
LOAD_DB="benchmark_stable"    # 写入库（表名固定为 cpu）
QUERY_DB="benchmark"          # 查询库（只读，需已灌好数据）
LOAD_WORKERS="6"
LOAD_BATCH="10000"
LOAD_LIMIT="0"                # 0 = 每轮灌整份数据文件
LOAD_INSERT_INTERVALS=""      # 非空则限速（如 "0.01"）
QUERY_WORKERS="8"
DLSQL_DIR=""
GRACE=120
RETRY_SLEEP=30
ROUND_GAP_LOAD=5
ROUND_GAP_QUERY=5
CREATE_TABLE=0
GEN_QUERIES=1
POC_SCALE="${POC_SCALE:-}"
POC_STALE_SCALE="${POC_STALE_SCALE:-}"
DRY_RUN=0
RESULTS_DIR=""

usage() {
  cat <<'EOF'
stability_24h.sh - 基于 poc/bench.sh 的长时间稳定性压测驱动。

用法： ./poc/scripts/stability_24h.sh [选项]

说明：
  写入独立库（--load-db，默认 benchmark_stable.cpu）；查询读 --query-db（默认 benchmark.cpu）。
  压力由 --load-workers / --query-workers 控制；测试使用自生成的临时 bench 配置，不改仓库共享配置。

选项：
  --hours N              总时长（小时，支持小数；默认 24）
  --flight-addr H:P      Arrow Flight SQL 地址（默认 localhost:8360）
  --http-addr H:P        HTTP 地址（默认 localhost:8361）
  --load-db NAME         写入库（默认 benchmark_stable）
  --query-db NAME        查询库（默认 benchmark）
  --load-workers N       写入并发（runner.workers 与 db-specific.num-workers，默认 6）
  --load-batch N         写入批量（默认 10000）
  --load-limit N         每轮最多写入行数（0=整份；默认 0）
  --load-insert-intervals V  写入限速（如 "0.01"，默认不限）
  --query-workers N      查询并发（默认 8）
  --scale N              fresh 数据集主机数（导出 POC_SCALE）
  --stale-scale N        stale 数据集主机数（导出 POC_STALE_SCALE）
  --dlsql-dir DIR        dlsql 所在目录（默认留空=用 PATH）
  --create-table         启动前用 dlsql 执行 poc/sql/create_stable.sql 建写入库/表
  --no-gen-queries       不自动生成查询文件（默认会检查并生成一次）
  --results DIR          结果目录（默认 ./results/stability-<时间戳>）
  --grace N              deadline 后等待在跑轮次的秒数（默认 120）
  --retry-sleep N        单轮失败后的退避秒数（默认 30）
  --round-gap N          每轮之间的间隔秒数（默认 5）
  --dry-run              只打印将要执行的内容，不真正运行
  -h, --help             显示帮助
EOF
}

log() { echo "[$(date '+%F %T')] $*" | tee -a "${MASTER_LOG:-/dev/stdout}"; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --hours) HOURS="$2"; shift 2 ;;
    --flight-addr) FLIGHT_ADDR="$2"; shift 2 ;;
    --http-addr) HTTP_ADDR="$2"; shift 2 ;;
    --load-db) LOAD_DB="$2"; shift 2 ;;
    --query-db) QUERY_DB="$2"; shift 2 ;;
    --load-workers) LOAD_WORKERS="$2"; shift 2 ;;
    --load-batch) LOAD_BATCH="$2"; shift 2 ;;
    --load-limit) LOAD_LIMIT="$2"; shift 2 ;;
    --load-insert-intervals) LOAD_INSERT_INTERVALS="$2"; shift 2 ;;
    --query-workers) QUERY_WORKERS="$2"; shift 2 ;;
    --scale) POC_SCALE="$2"; shift 2 ;;
    --stale-scale) POC_STALE_SCALE="$2"; shift 2 ;;
    --dlsql-dir) DLSQL_DIR="$2"; shift 2 ;;
    --create-table) CREATE_TABLE=1; shift ;;
    --no-gen-queries) GEN_QUERIES=0; shift ;;
    --results) RESULTS_DIR="$2"; shift 2 ;;
    --grace) GRACE="$2"; shift 2 ;;
    --retry-sleep) RETRY_SLEEP="$2"; shift 2 ;;
    --round-gap) ROUND_GAP_LOAD="$2"; ROUND_GAP_QUERY="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; usage; exit 1 ;;
  esac
done

[ -n "${POC_SCALE}" ] && export POC_SCALE
[ -n "${POC_STALE_SCALE}" ] && export POC_STALE_SCALE

RESULTS_DIR="${RESULTS_DIR:-./results/stability-$(date +%Y%m%d-%H%M%S)}"
CONF_DIR="${RESULTS_DIR}/configs"
mkdir -p "${CONF_DIR}"
MASTER_LOG="${RESULTS_DIR}/stability.log"
STATUS_FILE="${RESULTS_DIR}/status.txt"
ROUNDS_CSV="${RESULTS_DIR}/rounds.csv"
[ -f "${ROUNDS_CSV}" ] || echo "stream,round,start,end,duration_s,rc,log" > "${ROUNDS_CSV}"

FLIGHT_HOST="${FLIGHT_ADDR%:*}"; FLIGHT_PORT="${FLIGHT_ADDR##*:}"
DLSQL_BIN="${DLSQL_DIR:+${DLSQL_DIR}/}dlsql"

# ── 生成稳定性测试专用临时配置（不修改仓库里的 bench_*.yaml）─────────────
LOAD_CONF="${CONF_DIR}/bench_stable_load.yaml"
QUERY_CONF="${CONF_DIR}/bench_stable_query.yaml"

gen_bench_conf() { # gen_bench_conf <file> <db> <load_data> <run_queries> <query_workers>
  cat > "$1" <<EOF
# 由 stability_24h.sh 自动生成，仅用于稳定性测试。
flight_addr: ${FLIGHT_ADDR}
http_addr: ${HTTP_ADDR}
dlsql_dir: "${DLSQL_DIR}"
dldump_dir: ""
dlsql_extra_args: ""
dlsql_timeout: 300
dldump_timeout: 600
database: $2
create_db_table: false
gen_data: false
gen_queries: false
load_data: $3
create_rollup: false
run_queries: $4
query_workers: $5
EOF
}
gen_bench_conf "${LOAD_CONF}"  "${LOAD_DB}"  true  false "${QUERY_WORKERS}"
gen_bench_conf "${QUERY_CONF}" "${QUERY_DB}" false true  "${QUERY_WORKERS}"

# 写入侧额外覆盖：目标库 + 并发（+ 可选 batch/limit/限速）
LOAD_EXTRA="--loader.runner.db-name=${LOAD_DB} --loader.runner.workers=${LOAD_WORKERS} --loader.db-specific.num-workers=${LOAD_WORKERS} --loader.runner.batch-size=${LOAD_BATCH} --loader.db-specific.batch-size=${LOAD_BATCH}"
[ "${LOAD_LIMIT}" != "0" ] && LOAD_EXTRA="${LOAD_EXTRA} --loader.runner.limit=${LOAD_LIMIT}"
[ -n "${LOAD_INSERT_INTERVALS}" ] && LOAD_EXTRA="${LOAD_EXTRA} --loader.runner.insert-intervals=${LOAD_INSERT_INTERVALS}"
export LOAD_EXTRA_ARGS="${LOAD_EXTRA}"

START_TS=$(date +%s)
DEADLINE=$(( START_TS + $(awk -v h="${HOURS}" 'BEGIN{printf "%d", h*3600}') ))
STOP=0

log "=============================================================="
log "稳定性压测启动"
log "  时长        : ${HOURS} h（deadline $(date -d "@${DEADLINE}" '+%F %T')）"
log "  地址        : flight=${FLIGHT_ADDR} http=${HTTP_ADDR}"
log "  写入库/表   : ${LOAD_DB}.cpu"
log "  查询库/表   : ${QUERY_DB}.cpu"
log "  load 并发   : workers=${LOAD_WORKERS} batch=${LOAD_BATCH} limit=${LOAD_LIMIT} insert_intervals='${LOAD_INSERT_INTERVALS}'"
log "  query 并发  : workers=${QUERY_WORKERS}"
log "  临时配置    : ${CONF_DIR}/"
log "  结果目录    : ${RESULTS_DIR}"
log "=============================================================="

if [ "${DRY_RUN}" -eq 1 ]; then
  log "[dry-run] 写入配置: ${LOAD_CONF}"
  log "[dry-run] 查询配置: ${QUERY_CONF}"
  log "[dry-run] LOAD_EXTRA_ARGS=${LOAD_EXTRA_ARGS}"
  log "[dry-run] 循环：./poc/bench.sh ${LOAD_CONF} 与 ./poc/bench.sh ${QUERY_CONF}，直到 ${HOURS}h"
  exit 0
fi

# ── 前置：建写入库/表（可选）与生成查询文件（一次）──────────────────────
if [ "${CREATE_TABLE}" -eq 1 ]; then
  log "执行 poc/sql/create_stable.sql（建 ${LOAD_DB}.cpu）"
  timeout 300 "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" -u admin -p public \
    --load-file "${SCRIPT_DIR}/../sql/create_stable.sql" \
    || { log "ERROR: 建表失败"; exit 1; }
fi

QUERY_DIR="./generated_query/datalayers/cpu-only/poc"
if [ "${GEN_QUERIES}" -eq 1 ] && [ ! -f "${QUERY_DIR}/single-groupby-1-1-1.query" ]; then
  log "生成查询文件（一次）"
  ./poc/scripts/gen_queries_poc.sh
fi
[ -f "${QUERY_DIR}/single-groupby-1-1-1.query" ] || { log "ERROR: 查询文件不存在（${QUERY_DIR}）"; exit 1; }

count_stream() { local n; n=$(grep -c "^$1," "${ROUNDS_CSV}" 2>/dev/null || true); echo "${n:-0}"; }
count_fail()   { awk -F, -v s="$1" '$1==s && $6!=0' "${ROUNDS_CSV}" 2>/dev/null | wc -l | tr -d ' '; }

write_status() {
  local lr qr
  lr=$(count_stream load); qr=$(count_stream query)
  {
    echo "start=$(date -d "@${START_TS}" '+%F %T')"
    echo "now=$(date '+%F %T')"
    echo "elapsed=$(( $(date +%s) - START_TS ))s remaining=$(( DEADLINE - $(date +%s) < 0 ? 0 : DEADLINE - $(date +%s) ))s"
    echo "load_rounds_done=${lr}"
    echo "query_rounds_done=${qr}"
    [ -f "${RESULTS_DIR}/load.current.pid" ]  && echo "load_current_pid=$(cat "${RESULTS_DIR}/load.current.pid")"
    [ -f "${RESULTS_DIR}/query.current.pid" ] && echo "query_current_pid=$(cat "${RESULTS_DIR}/query.current.pid")"
  } > "${STATUS_FILE}.tmp" && mv "${STATUS_FILE}.tmp" "${STATUS_FILE}"
}

run_round() {
  local stream="$1" config="$2" round="$3" ts logfile start end dur rc pid
  ts=$(date +%Y%m%d-%H%M%S); logfile="${RESULTS_DIR}/${stream}-round${round}-${ts}.log"
  start=$(date +%s)
  log "[${stream}] round ${round} 开始 -> $(basename "${logfile}")"
  setsid ./poc/bench.sh "${config}" >"${logfile}" 2>&1 &
  pid=$!; echo "${pid}" > "${RESULTS_DIR}/${stream}.current.pid"
  wait "${pid}"; rc=$?
  rm -f "${RESULTS_DIR}/${stream}.current.pid"
  end=$(date +%s); dur=$(( end - start ))
  echo "${stream},${round},$(date -d "@${start}" '+%F %T'),$(date -d "@${end}" '+%F %T'),${dur},${rc},${logfile##*/}" >> "${ROUNDS_CSV}"
  if [ "${rc}" -eq 0 ]; then log "[${stream}] round ${round} 完成（${dur}s）";
  else log "[${stream}] round ${round} 失败 rc=${rc}（${dur}s），退避 ${RETRY_SLEEP}s"; sleep "${RETRY_SLEEP}"; fi
}

loop() {
  local stream="$1" config="$2" gap="$3" round=0
  while [ "${STOP}" -eq 0 ] && [ "$(date +%s)" -lt "${DEADLINE}" ]; do
    round=$(( round + 1 ))
    run_round "${stream}" "${config}" "${round}"
    [ "${STOP}" -eq 0 ] && sleep "${gap}"
  done
  log "[${stream}] 循环结束（共 ${round} 轮）"
}

kill_running() {
  local sig="$1" f pid
  for f in "${RESULTS_DIR}"/*.current.pid; do
    [ -f "${f}" ] || continue
    pid=$(cat "${f}" 2>/dev/null || true)
    [ -n "${pid}" ] && kill -"${sig}" -- "-${pid}" 2>/dev/null || true
  done
}
on_signal() { log "收到停止信号，正在结束 ..."; STOP=1; kill_running TERM; }
trap on_signal INT TERM

loop load  "${LOAD_CONF}"  "${ROUND_GAP_LOAD}"  & LP=$!
loop query "${QUERY_CONF}" "${ROUND_GAP_QUERY}" & QP=$!

while [ "${STOP}" -eq 0 ] && [ "$(date +%s)" -lt "${DEADLINE}" ]; do write_status; sleep 30; done
[ "${STOP}" -eq 0 ] && { log "已到达 ${HOURS}h deadline，停止发起新轮次"; STOP=1; }

for _ in $(seq 1 "${GRACE}"); do pgrep -f "poc/bench.sh" >/dev/null 2>&1 || break; sleep 1; done
kill_running TERM; sleep 3; kill_running KILL
wait "${LP}" 2>/dev/null; wait "${QP}" 2>/dev/null
write_status

log "=============================================================="
log "稳定性压测结束"
log "  实际运行  : $(( $(date +%s) - START_TS ))s（约 $(awk -v s="$(( $(date +%s) - START_TS ))" 'BEGIN{printf "%.2f", s/3600}') h）"
log "  load 轮次 : $(count_stream load)（失败 $(count_fail load)）"
log "  query 轮次: $(count_stream query)（失败 $(count_fail query)）"
log "  结果目录  : ${RESULTS_DIR}"
log "=============================================================="
