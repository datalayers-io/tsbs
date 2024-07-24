#!/bin/bash

FORMAT=$1
USE_CASE=$2
SCALE=$3
DURATION=$4
SEED=$5

START_TIMESTAMP="2016-01-01T00:00:00Z"
# 计算结束时间
case $DURATION in
    31d)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 31 days" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    4d)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 4 days" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    3h)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 3 hours" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    3m)
        END_TIMESTAMP=$(date -u -d "$START_TIMESTAMP + 3 minutes" +"%Y-%m-%dT%H:%M:%SZ")
        ;;
    *)
        echo "Invalid duration specified. Use 31d, 4d, 3h, or 3m."
        exit 1
        ;;
esac

OUT="./data/$FORMAT/$USE_CASE-$SCALE-$DURATION.data"

# 提取目录路径
OUT_DIR=$(dirname "$OUT")
# 检查目录是否存在，如果不存在则创建
if [ ! -d "$OUT_DIR" ]; then
  mkdir -p "$OUT_DIR"
  echo "Directory $OUT_DIR created."
else
  echo "Directory $OUT_DIR already exists."
fi

./bin/tsbs_generate_data \
    --format="$FORMAT" \
    --use-case="$USE_CASE" \
    --scale=$SCALE \
    --seed=$SEED \
    --timestamp-start="$START_TIMESTAMP" \
    --timestamp-end="$END_TIMESTAMP" \
    --file="$OUT"
