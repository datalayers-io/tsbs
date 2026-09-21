#!/bin/bash

# Run all 15 POC query types against Datalayers, print live progress, and
# aggregate per-query results (query label, SQL hints, mean latency, QPS) into
# a results directory.
#
# Usage:
#   ./poc/scripts/run_all_queries_poc.sh <workers> [print-interval] [query-dir]
#
# Env:
#   SQL_ENDPOINT  Arrow Flight SQL 地址（默认 localhost:8360）
#   RESULTS_DIR   结果输出目录（默认 ./results/poc-<时间戳>）
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_DIR}"

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <workers> [print-interval] [query-dir]" >&2
  exit 1
fi

WORKERS="$1"
PRINT_INTERVAL="${2:-100}"
QUERY_DIR="${3:-./generated_query/datalayers/cpu-only/poc}"
SQL_ENDPOINT="${SQL_ENDPOINT:-localhost:8360}"
RESULTS_DIR="${RESULTS_DIR:-./results/poc-$(date +%Y%m%d-%H%M%S)}"

mkdir -p "${RESULTS_DIR}"

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

SUMMARY_TSV="${RESULTS_DIR}/summary.tsv"
printf "query\tparallel_degree\tskip_rollup\tfast_last_buckets\tmean_ms\tqps\n" > "${SUMMARY_TSV}"

TOTAL="${#QUERY_TYPES[@]}"
for i in "${!QUERY_TYPES[@]}"; do
  idx=$((i + 1))
  qt="${QUERY_TYPES[$i]}"
  f="${QUERY_DIR}/${qt}.query"

  if [ ! -f "${f}" ]; then
    echo "[${idx}/${TOTAL}] WARN: 缺少查询文件 ${f}，跳过" >&2
    printf "%s\tNA\tNA\tNA\tNA\tNA\n" "${qt}" >> "${SUMMARY_TSV}"
    continue
  fi

  log="${RESULTS_DIR}/${qt}.log"
  echo "[${idx}/${TOTAL}] running ${qt} (workers=${WORKERS}) ..."

  # stdout -> per-query log；stderr（实时进度）同时落到 stderr.log 和终端
  ./bin/tsbs_run_queries_datalayers \
    --file="${f}" \
    --sql-endpoint="${SQL_ENDPOINT}" \
    --workers="${WORKERS}" \
    --burn-in=10 \
    --print-interval="${PRINT_INTERVAL}" \
    > "${log}" 2> >(tee "${log}.stderr" >&2) || true

  hdr=$(grep -m1 "query file" "${log}" || true)
  # parallel_degree：从 SQL hint 解析；没有 parallel_degree hint 时为空 -> N/A
  pd=$(printf '%s' "${hdr}" | grep -oE "parallel_degree=\[[0-9, ]*\]" \
        | sed -E 's/parallel_degree=//; s/\[|\]//g' | tr -s ' ' ',' | sed -E 's/^,|,$//g' || true)
  [ -n "${pd}" ] || pd="N/A"
  # skip_rollup：只有 SQL hint 中出现 skip_rollup=1 才显示 true，否则 N/A
  sr_raw=$(printf '%s' "${hdr}" | grep -oE "skip_rollup=\[(true|false)\]" \
        | sed -E 's/skip_rollup=//; s/\[|\]//g' || true)
  sr="N/A"
  case "${sr_raw}" in
    *true*) sr="true" ;;
  esac
  # fast_last_buckets：只有 SQL hint 中出现 fast_last_buckets=1 才显示 true，否则 N/A
  flb_raw=$(printf '%s' "${hdr}" | grep -oE "fast_last_buckets=\[(true|false)\]" \
        | sed -E 's/fast_last_buckets=//; s/\[|\]//g' || true)
  flb="N/A"
  case "${flb_raw}" in
    *true*) flb="true" ;;
  esac
  mean=$(grep -oE "mean: +[0-9.]+ms" "${log}" | head -1 | grep -oE "[0-9.]+" | head -1 || true)
  qps=$(grep -oE "Overall query rate +[0-9.]+ queries/sec" "${log}" | head -1 | grep -oE "[0-9.]+" || true)

  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "${qt}" "${pd}" "${sr}" "${flb}" "${mean:-N/A}" "${qps:-N/A}" >> "${SUMMARY_TSV}"
done

echo ""
echo "=== POC Query Summary ==="
printf "%-24s %-13s %-10s %-16s %12s %12s\n" "query" "parallel_degree" "skip_rollup" "fast_last_buckets" "mean(ms)" "qps"
printf "%-24s %-13s %-10s %-16s %12s %12s\n" "------------------------" "-------------" "----------" "----------------" "------------" "------------"
while IFS=$'\t' read -r q pd sr flb mean qps; do
  [ "${q}" = "query" ] && continue
  printf "%-24s %-13s %-10s %-16s %12s %12s\n" "${q}" "${pd}" "${sr}" "${flb}" "${mean}" "${qps}"
done < "${SUMMARY_TSV}"
echo ""
echo "summary  : ${SUMMARY_TSV}"
echo "logs     : ${RESULTS_DIR}/"
