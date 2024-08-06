#!/bin/bash

SCENARIO=$1
QUERY_TYPE=$2
WORKERS=$3

QUERY_FILE="./generated_query/influx/cpu-only/$SCENARIO/$QUERY_TYPE.query"

./bin/tsbs_run_queries_influx \
    --file="$QUERY_FILE" \
    --urls="http://10.0.0.10:8086" \
    --workers=$WORKERS 
