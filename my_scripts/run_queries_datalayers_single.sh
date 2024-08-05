#!/bin/bash

SCENARIO=$1
QUERY_TYPE=$2
WORKERS=$3

QUERY_FILE="./query/datalayers/cpu-only/$SCENARIO/$QUERY_TYPE.query"
MIDDLE_PATH=$(echo "$QUERY_FILE" | sed 's|^\./query/\(.*\)\.query$|\1|')
RESULTS_FILE="./query_results/$MIDDLE_PATH.json"

RESULTS_DIR=$(dirname $RESULTS_FILE)
if [ ! -d "$RESULTS_DIR" ]; then
  mkdir -p "$RESULTS_DIR"
fi

./bin/tsbs_run_queries_datalayers \
    --file="$QUERY_FILE" \
    --results-file="$RESULTS_FILE" \
    --sql-endpoint="10.0.0.10:8360" \
    --workers=$WORKERS 
