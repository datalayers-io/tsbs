#!/usr/bin/env bash
#
# pack.sh - Collect the built Datalayers TSBS binaries and the whole poc/
# subtree (POC scripts, hint config, POC load configs, README) into one
# self-contained, runnable bundle. The bundle preserves the repo layout
# (./bin, ./poc/...), so the scripts can be run directly as-is. The generic
# my_scripts / load_config/datalayers are intentionally NOT included: the POC
# flow is self-contained under poc/ and depends only on bin/.
#
# Usage:
#   ./poc/scripts/pack.sh [OUTPUT_DIR]
#
# Default OUTPUT_DIR: <repo>/dist/datalayers-<hostname>-<YYYYmmdd-HHMMSS>
#
# Run on the target machine afterwards (from the bundle root):
#   ./poc/scripts/gen_data_poc.sh [stale]
#   ./poc/scripts/gen_queries_poc.sh
#   ./poc/scripts/rewrite_queries_poc.sh
#   ./poc/scripts/load_data_poc.sh [stale]
#   ./poc/scripts/run_queries_poc.sh <workers> <query-number>
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# All Datalayers binaries produced by `make all`.
BINARIES=(
  tsbs_generate_data
  tsbs_generate_queries
  tsbs_load
  tsbs_run_queries_datalayers
  rewrite_query_hints_config
)

# Generic Datalayers pipeline scripts from my_scripts are intentionally NOT
# included; the POC flow is self-contained under poc/.

# POC scripts under poc/scripts.
POC_SCRIPTS=(
  gen_data_poc.sh
  gen_queries_poc.sh
  load_data_poc.sh
  pack.sh
  rewrite_queries_poc.sh
  run_queries_poc.sh
  run_all_queries_poc.sh
  bench_common.sh
  test_alter.sh
  compute_compression_ratio.sh
)

# POC SQL files under poc/sql.
POC_SQL=(
  create.sql
  alter.sql
  sample.sql
)

# POC root files (bench entry, config, README).
POC_FILES=(
  bench.sh
  bench_config.yaml
  README.md
)

OUTPUT_DIR="${1:-${REPO_DIR}/dist/datalayers-$(hostname)-$(date +%Y%m%d-%H%M%S)}"

# Every binary must have been built before packaging.
missing=0
for b in "${BINARIES[@]}"; do
  if [ ! -x "${REPO_DIR}/bin/${b}" ]; then
    echo "ERROR: missing binary bin/${b} - run 'make all' first" >&2
    missing=1
  fi
done
[ "${missing}" -eq 0 ] || exit 1

mkdir -p \
  "${OUTPUT_DIR}/bin" \
  "${OUTPUT_DIR}/poc/scripts" \
  "${OUTPUT_DIR}/poc/load_config" \
  "${OUTPUT_DIR}/poc/sql"

for b in "${BINARIES[@]}"; do
  cp "${REPO_DIR}/bin/${b}" "${OUTPUT_DIR}/bin/"
done

for s in "${POC_SCRIPTS[@]}"; do
  cp "${REPO_DIR}/poc/scripts/${s}" "${OUTPUT_DIR}/poc/scripts/"
done
cp "${REPO_DIR}/poc/scripts/poc_hints.yaml" "${OUTPUT_DIR}/poc/scripts/"
cp "${REPO_DIR}/poc/load_config/"*.yaml "${OUTPUT_DIR}/poc/load_config/"
for f in "${POC_SQL[@]}"; do
  cp "${REPO_DIR}/poc/sql/${f}" "${OUTPUT_DIR}/poc/sql/"
done
for f in "${POC_FILES[@]}"; do
  cp "${REPO_DIR}/poc/${f}" "${OUTPUT_DIR}/poc/"
done

echo "Packed Datalayers POC bundle -> ${OUTPUT_DIR}"
echo "  bin/             : ${#BINARIES[@]} binaries"
echo "  poc/scripts/     : ${#POC_SCRIPTS[@]} scripts + poc_hints.yaml"
echo "  poc/load_config/ : $(ls "${OUTPUT_DIR}/poc/load_config/" | wc -l) configs"
echo "  poc/sql/         : ${#POC_SQL[@]} files (create.sql / alter.sql / sample.sql)"
echo "  poc/ root        : ${#POC_FILES[@]} files (bench.sh / bench_config.yaml / README)"
echo ""
echo "Usage on the target machine:"
echo "  cd ${OUTPUT_DIR}"
echo "  ./poc/bench.sh          # 按 poc/bench_config.yaml 一键压测"
echo "  # 或分步执行："
echo "  ./poc/scripts/gen_data_poc.sh [stale]"
echo "  ./poc/scripts/gen_queries_poc.sh"
echo "  ./poc/scripts/rewrite_queries_poc.sh"
echo "  ./poc/scripts/load_data_poc.sh [stale]"
echo "  ./poc/scripts/run_queries_poc.sh <workers> <query-number>"
echo "  ./poc/scripts/run_all_queries_poc.sh <workers>"
echo "  ./poc/scripts/test_alter.sh"
echo "  ./poc/scripts/compute_compression_ratio.sh"
