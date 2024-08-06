#!/bin/bash

SCENARIO=$1
QUERY_TYPE=$2
WORKERS=$3

QUERY_FILE="./generated_query/datalayers/cpu-only/$SCENARIO/$QUERY_TYPE.query"

./bin/tsbs_run_queries_datalayers \
    --file="$QUERY_FILE" \
    --sql-endpoint="10.0.0.10:8360" \
    --workers=$WORKERS 
