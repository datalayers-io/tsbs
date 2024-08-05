#!/bin/bash

SCENARIO=$1
WORKERS=$2
QUERY_ID=$3

case $SCENARIO in
  1)
    SCENARIO="small"
    ;;
  2)
    SCENARIO="large"
    ;;
  *)
    echo "Invalid SCENARIO: $SCENARIO"
    echo "SCENARIO options: 1, 2"
    exit 1
    ;;
esac

case $QUERY_ID in
  1)
    QUERY_TYPE="single-groupby-1-1-1"
    ;;
  2)
    QUERY_TYPE="single-groupby-1-1-12"
    ;;
  3)
    QUERY_TYPE="single-groupby-1-8-1"
    ;;
  4)
    QUERY_TYPE="single-groupby-5-1-1"
    ;;
  5)
    QUERY_TYPE="single-groupby-5-1-12"
    ;;
  6)
    QUERY_TYPE="single-groupby-5-8-1"
    ;;
  7)
    QUERY_TYPE="cpu-max-all-1"
    ;;
  8)
    QUERY_TYPE="cpu-max-all-8"
    ;;
  9)
    QUERY_TYPE="double-groupby-1"
    ;;
  10)
    QUERY_TYPE="double-groupby-5"
    ;;
  11)
    QUERY_TYPE="double-groupby-all"
    ;;
  12)
    QUERY_TYPE="high-cpu-1"
    ;;
  13)
    QUERY_TYPE="high-cpu-all"
    ;;
  14)
    QUERY_TYPE="lastpoint"
    ;;
  15)
    QUERY_TYPE="groupby-orderby-limit"
    ;;
  *)
    echo "Invalid QUERY_ID: $QUERY_ID"
    echo "QUERY_ID options: 1 ~ 15"
    exit 1
    ;;
esac

./my_scripts/run_queries_influx_single.sh $SCENARIO $QUERY_TYPE $WORKERS
