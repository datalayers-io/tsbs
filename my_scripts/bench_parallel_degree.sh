#!/usr/bin/env bash
#
# bench_parallel_degree.sh - Probe optimal parallel_degree for TSBS queries.
#
# Usage:
#   bash bench_parallel_degree.sh <DATALAYERS_REPO> <SERVER_PORT> [small|large]
#
#   DATALAYERS_REPO  Path to Datalayers repo (contains target/release/dlsql).
#   SERVER_PORT      Datalayers server port (e.g. 8460).
#   scenario         small (default) or large.
#
# Reads per-query SQL from:
#   <script_dir>/../generated_query/datalayers/cpu-only/<scenario>/
#
# Writes results to:
#   <script_dir>/optimal_parallel_degree.txt
#
set -euo pipefail

REPO="${1:?usage: $0 <DATALAYERS_REPO> <SERVER_PORT> [small|large]}"
PORT="${2:?usage: $0 <DATALAYERS_REPO> <SERVER_PORT> [small|large]}"
SCENARIO="${3:-small}"
DLSQL="${REPO}/target/release/dlsql"
HOST="127.0.0.1"
DB="benchmark"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GEN_DIR="${SCRIPT_DIR}/../generated_query/datalayers/cpu-only/${SCENARIO}"
TMPDIR="${SCRIPT_DIR}/.bench_parallel_$$"
OUTFILE="${SCRIPT_DIR}/optimal_parallel_degree_${SCENARIO}.txt"

COPIES=5
SAMPLES=3

if [ "$SCENARIO" = "large" ]; then
    DEGREES=(1 2 4 8 16 32 64)
else
    DEGREES=(1 2 3 4 6 8 12 16 32 64)
fi

# ── pre-flight ──────────────────────────────────────────
[ -x "$DLSQL" ] || { echo "ERROR: dlsql not found at $DLSQL" >&2; exit 1; }
[ -d "$GEN_DIR" ] || { echo "ERROR: $GEN_DIR not found" >&2; exit 1; }

# ── extract SQL from gob files ──────────────────────────
extract_sql() {
    python3 -c "
import sys, re
raw = open(sys.argv[1], 'rb').read()
text = raw.decode('latin-1', errors='ignore')
# Handle CTE: find WITH ... SELECT
m = re.search(r'(WITH\s+.*?SELECT.*?)(?=\x00)', text, re.DOTALL | re.IGNORECASE)
if m:
    sql = re.sub(r'\s+', ' ', m.group(1)).strip()
else:
    m = re.search(r'SELECT.*?(?=\x00)', text, re.DOTALL)
    if m:
        sql = re.sub(r'\s+', ' ', m.group(0)).strip()
    else:
        print('')
        sys.exit(0)
# Remove any existing set_var hints
sql = re.sub(r'/\*\+.*?\*/\s*', '', sql).strip()
print(sql)
" "$1"
}

declare -A QUERIES
echo "Extracting SQLs from $GEN_DIR ..." >&2
for f in "$GEN_DIR"/*.query; do
    qname="$(basename "$f" .query)"
    sql="$(extract_sql "$f")"
    [ -n "$sql" ] || continue
    QUERIES["$qname"]="$sql"
done
echo "Extracted ${#QUERIES[@]} query types." >&2

# ── generate load files ─────────────────────────────────
rm -rf "$TMPDIR" && mkdir -p "$TMPDIR"
total=0
for qname in "${!QUERIES[@]}"; do
    for deg in "${DEGREES[@]}"; do
        f="$TMPDIR/${qname}_p${deg}.sql"
        # Insert hint right after SELECT
        hinted="${QUERIES[$qname]}"
        hinted="SELECT /*+ set_var(parallel_degree=$deg) */${hinted#SELECT}"
        for i in $(seq 1 $COPIES); do
            echo "$hinted;" >> "$f"
        done
        total=$((total + 1))
    done
done
echo "Generated $total load files in $TMPDIR" >&2

# ── measure ─────────────────────────────────────────────
measure() {
    local f="$1"
    local start end
    start=$(date +%s%N)
    "$DLSQL" -h "$HOST" -P "$PORT" -d "$DB" --max-display-rows 1 --load-file "$f" >/dev/null 2>&1
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

echo ""
echo "Running ($SAMPLES samples x $COPIES copies per file) ... $(date)" >&2

> "$OUTFILE"
{
    printf "%-26s" "query"
    for deg in "${DEGREES[@]}"; do printf " p=%-3d" "$deg"; done
    printf " | best_N\n"
    printf "%s" "$(printf '%.0s-' {1..26})"
    ncols=$((${#DEGREES[@]} * 6))
    printf "%s" "$(printf '%.0s-' $(seq 1 $ncols))"
    printf -- "---------\n"
} >> "$OUTFILE"

for qname in $(printf '%s\n' "${!QUERIES[@]}" | sort); do
    printf "  %-22s" "$qname" >&2

    declare -A per_query_ms=()

    for deg in "${DEGREES[@]}"; do
        f="$TMPDIR/${qname}_p${deg}.sql"
        samples=""
        for s in $(seq 1 $SAMPLES); do
            t=$(measure "$f")
            samples="$samples $t"
        done

        # Drop min and max if SAMPLES >= 4, else keep all
        sorted=$(echo "$samples" | tr ' ' '\n' | sort -n)
        if [ $SAMPLES -ge 4 ]; then
            trimmed=$(echo "$sorted" | sed '1d;$d')
        else
            trimmed="$sorted"
        fi
        sum=0; cnt=0
        for t in $trimmed; do sum=$((sum + t)); cnt=$((cnt + 1)); done
        per_query=$((sum / cnt / COPIES))
        per_query_ms[$deg]=$per_query
    done

    # Find best N: lowest time wins
    best_ms=99999999
    best_n=0
    for deg in "${DEGREES[@]}"; do
        t=${per_query_ms[$deg]}
        if [ "$t" -lt "$best_ms" ]; then
            best_ms=$t
            best_n=$deg
        fi
    done

    # Output row
    printf "%-26s" "$qname" >> "$OUTFILE"
    for deg in "${DEGREES[@]}"; do
        t=${per_query_ms[$deg]}
        if [ "$t" -eq "$best_ms" ]; then
            printf " \033[1m%-4d\033[0m" "$t" >> "$OUTFILE"
        else
            printf " %-4d " "$t" >> "$OUTFILE"
        fi
    done
    printf " | %d\n" "$best_n" >> "$OUTFILE"
    echo " → N=$best_n (${per_query_ms[$best_n]}ms)" >&2

    unset per_query_ms
done

echo "" >> "$OUTFILE"
echo "# Recommended parallel_degree per query (scenario: ${SCENARIO})" >> "$OUTFILE"

rm -rf "$TMPDIR"
echo ""
echo "Done → $OUTFILE" >&2
cat "$OUTFILE"
