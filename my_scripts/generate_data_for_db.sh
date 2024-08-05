#!/bin/bash

SEED=42

generate_data() {
  ./my_scripts/generate_data.sh "$@"
}

# 检查是否提供了参数
if [ -z "$1" ]; then
  echo "Usage: $0 {all|datalayers|influx|timescaledb}"
  exit 1
fi

TARGET_DB=$1

# 定义函数来生成数据
generate_data_for_db() {
  local db=$1
  generate_data $db cpu-only 100 31d $SEED  
  generate_data $db cpu-only 4000 4d $SEED
  generate_data $db cpu-only 100000 3h $SEED
  generate_data $db cpu-only 1000000 3m $SEED
  generate_data $db cpu-only 10000000 3m $SEED
}

# 根据参数决定为哪些数据库生成数据
case $TARGET_DB in
  all)
    generate_data_for_db datalayers
    generate_data_for_db influx
    generate_data_for_db timescaledb
    ;;
  datalayers)
    generate_data_for_db datalayers
    ;;
  influx)
    generate_data_for_db influx
    ;;
  timescaledb)
    generate_data_for_db timescaledb
    ;;
  *)
    echo "Invalid option: $TARGET_DB"
    echo "Usage: $0 {all|datalayers|influx|timescaledb}"
    exit 1
    ;;
esac
