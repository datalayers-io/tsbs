#!/usr/bin/env bash
#
# compute_compression_ratio.sh - 计算 Datalayers 数据文件（SST）相对原始 CSV 的压缩率。
#
# 流程：
#   1. 探测 datalayers 联通性 / dlsql / dldump（来自 bench_config.yaml）
#   2. drop cpu_sample（幂等）
#   3. 用 dlsql --load-file 执行 poc/sql/sample.sql：
#        建 cpu_sample 表（字段同 cpu，PARTITIONS=1，memtable 8GiB）
#        -> INSERT ... SELECT 从 cpu 取 1000 万行 -> FLUSH TABLE cpu_sample SYNC
#        -> SELECT table,file_size FROM information_schema.sst_files
#   4. 从 sst_files 输出求 file_size 总和 = datalayers 数据文件大小
#   5. 用 dldump -f csv 导出 cpu_sample 到 CSV，得到 csv 文件大小
#   6. 计算三组比值：
#        - datalayers 数据文件是 csv 文件的百分之多少
#        - csv 文件大小是 datalayers 数据文件的多少倍
#        - datalayers:csv 比值（datalayers 取 1）
#
# 用法：
#   ./poc/scripts/compute_compression_ratio.sh [config.yaml]
#   （默认读取 ./poc/bench_config.yaml）
#
# 输出目录：./results/compression-<时间戳>/
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

RESULTS_DIR="./results/compression-$(date +%Y%m%d-%H%M%S)"
mkdir -p "${RESULTS_DIR}"
SAMPLE_OUT="${RESULTS_DIR}/sample.out"

# ── 1. drop cpu_sample（保证幂等，避免重复灌数）──────────────────────────
info "drop table if exists ${DATABASE}.cpu_sample"
timeout "${DLSQL_TIMEOUT}s" "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" \
  -d "${DATABASE}" -e "drop table if exists ${DATABASE}.cpu_sample" \
  || die "drop cpu_sample 失败"

# ── 2. 执行 sample.sql：建表 + 灌 1000 万行 + flush + 查 sst_files ─────────
info "执行 ${REPO_DIR}/poc/sql/sample.sql（建表/灌数据/flush/查 sst_files，超时 ${DLSQL_TIMEOUT}s）"
timeout "${DLSQL_TIMEOUT}s" "${DLSQL_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" \
  -d "${DATABASE}" --load-file "${REPO_DIR}/poc/sql/sample.sql" | tee "${SAMPLE_OUT}"

# ── 3. 求和 sst_files 的 file_size（datalayers 数据文件大小）──────────────
# 输出形如：| cpu_sample | 123456 | ...；用 | 分割取第 3 列（file_size）求和。
DL_SIZE="$(awk -F'|' '{ gsub(/[^0-9]/,"",$3); sum+=$3 } END{ print sum+0 }' "${SAMPLE_OUT}")"
if [ "${DL_SIZE}" -le 0 ]; then
  echo "WARN: 未从 sst_files 输出解析到 file_size，原始输出：" >&2
  sed -n '/sst_files\|file_size\|cpu_sample/p' "${SAMPLE_OUT}" >&2 || true
fi
echo "datalayers cpu_sample 数据文件大小: ${DL_SIZE} bytes"

# ── 4. dldump 导出 cpu_sample 到 CSV ──────────────────────────────────────
DUMP_DIR="${RESULTS_DIR}/dump"
rm -rf "${DUMP_DIR}"
info "dldump 导出 ${DATABASE}.cpu_sample -> csv（超时 ${DLDUMP_TIMEOUT}s）"
timeout "${DLDUMP_TIMEOUT}s" "${DLDUMP_BIN}" -h "${FLIGHT_HOST}" -P "${FLIGHT_PORT}" \
  -u admin -p public -d "${DATABASE}" -t cpu_sample -o "${DUMP_DIR}" -f csv \
  || die "dldump 导出失败"

CSV="${DUMP_DIR}/${DATABASE}_cpu_sample.csv"
[ -f "${CSV}" ] || die "CSV 文件不存在: ${CSV}"
CSV_SIZE="$(stat -c %s "${CSV}")"
echo "csv 文件大小: ${CSV_SIZE} bytes (${CSV})"

# ── 5. 计算三组比值 ───────────────────────────────────────────────────────
PCT="$(awk -v a="${DL_SIZE}" -v b="${CSV_SIZE}" 'BEGIN{ printf "%.2f", a/b*100 }')"
MULT="$(awk -v a="${DL_SIZE}" -v b="${CSV_SIZE}" 'BEGIN{ printf "%.2f", b/a }')"
RATIO="$(awk -v a="${DL_SIZE}" -v b="${CSV_SIZE}" 'BEGIN{ printf "%.4f", b/a }')"

echo ""
echo "=== 压缩率结果 ==="
echo "datalayers 数据文件大小 : ${DL_SIZE} bytes"
echo "csv 文件大小            : ${CSV_SIZE} bytes"
echo ""
echo "1) datalayers 数据文件是 csv 文件的      ${PCT}%"
echo "2) csv 文件大小是 datalayers 数据文件的   ${MULT} 倍"
echo "3) datalayers:csv 比值（datalayers=1）   1 : ${RATIO}"
echo ""
echo "结果目录: ${RESULTS_DIR}"
