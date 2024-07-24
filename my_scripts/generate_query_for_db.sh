#!/bin/bash

SEED=42

alias generate_query='./my_scripts/common/generate_query.sh'

# 检查是否提供了参数
if [ -z "$1" ]; then
  echo "Usage: $0 {all|datalayers|influx|timescaledb}"
  exit 1
fi

TARGET_DB=$1

# 定义所有支持的查询类型
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
  "high-cpu-all"
  "high-cpu-1"
  "lastpoint"
  "groupby-orderby-limit"
)

# 定义函数来生成查询
generate_query_for_db() {
  local db=$1
  for query_type in "${QUERY_TYPES[@]}"; do
    generate_query $db cpu-only 100 31d $SEED $query_type 
    generate_query $db cpu-only 4000 4d $SEED $query_type
    generate_query $db cpu-only 100000 3h $SEED $query_type
    generate_query $db cpu-only 1000000 3m $SEED $query_type
    generate_query $db cpu-only 10000000 3m $SEED $query_type
  done
}

# 根据参数决定为哪些数据库生成数据
case $TARGET_DB in
  all)
    generate_query_for_db datalayers
    generate_query_for_db influx
    generate_query_for_db timescaledb
    ;;
  datalayers)
    generate_query_for_db datalayers
    ;;
  influx)
    generate_query_for_db influx
    ;;
  timescaledb)
    generate_query_for_db timescaledb
    ;;
  *)
    echo "Invalid option: $TARGET_DB"
    echo "Usage: $0 {all|datalayers|influx|timescaledb}"
    exit 1
    ;;
esac
