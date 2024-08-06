#!/bin/bash

FORMAT=$1
USE_CASE=$2
SCALE=$3
DURATION=$4
QUERY_TYPE=$5
NUM_QUERIES=$6
SEED=$7

START_TIMESTAMP="2016-01-01T00:00:00Z"
# 计算结束时间
case $DURATION in
    31d)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 31 days + 1 second" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    4d)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 4 days + 1 second" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    3h)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 3 hours + 1 second" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    3m)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 3 minutes + 1 second" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    *)
        echo "Invalid duration specified. Use 31d, 4d, 3h, or 3m."
        exit 1
        ;;
esac

# 判断 SCALE 的值来设置 QUERY_SIZE
case $SCALE in
  4000)
    QUERY_SIZE="large"
    ;;
  100)
    QUERY_SIZE="small"
    ;;
  *)
    echo "Invalid SCALE value: $SCALE. Only 4000 (large) and 100 (small) are allowed."
    exit 1
    ;;
esac

OUT="./generated_query/$FORMAT/$USE_CASE/$QUERY_SIZE/$QUERY_TYPE.query"

# 提取目录路径
OUT_DIR=$(dirname "$OUT")
# 检查目录是否存在，如果不存在则创建
if [ ! -d "$OUT_DIR" ]; then
  mkdir -p "$OUT_DIR"
fi

echo "Generating query $OUT ..."

./bin/tsbs_generate_queries \
  --format="$FORMAT" \
  --use-case="$USE_CASE" \
  --query-type="$QUERY_TYPE" \
  --queries=$NUM_QUERIES \
  --scale=$SCALE \
  --seed=$SEED \
  --timestamp-start="$START_TIMESTAMP" \
  --timestamp-end="$END_TIMESTAMP" \
  --file="$OUT"
