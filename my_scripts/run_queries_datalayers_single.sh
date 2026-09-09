#!/bin/bash

unset http_proxy https_proxy

SCENARIO=$1
QUERY_TYPE=$2
WORKERS=$3

QUERY_FILE="./generated_query/datalayers/cpu-only/$SCENARIO/$QUERY_TYPE.query"

./bin/tsbs_run_queries_datalayers \
    --file="$QUERY_FILE" \
    --sql-endpoint="localhost:8360" \
    --workers=$WORKERS \
    --burn-in=10 
