#!/bin/bash

#!/bin/bash

SCENARIO=$1
WORKERS=$2
BATCH_SIZE=$3
CHUNK_TIME=$4

case $SCENARIO in
  1)
    FILE="cpu-only-100-31d.data"
    ;;
  2)
    FILE="cpu-only-4000-4d.data"
    ;;
  3)
    FILE="cpu-only-100000-3h.data"
    ;;
  4)
    FILE="cpu-only-1000000-3m.data"
    ;;
  5)
    FILE="cpu-only-10000000-3m.data"
    ;;
  *)
    echo "Invalid SCENARIO: $SCENARIO"
    echo "SCENARIO options: 1, 2, 3, 4, 5"
    exit 1
    ;;
esac

FILE_WITHOUT_EXTENSION=$(echo $FILE | sed 's/\.data.*//')
RESULTS_FILE="./load_results/timescaledb/$FILE_WITHOUT_EXTENSION.json"
FILE="./data/timescaledb/$FILE"

RESULTS_DIR=$(dirname $RESULTS_FILE)
if [ ! -d "$RESULTS_DIR" ]; then
  mkdir -p "$RESULTS_DIR"
fi

./bin/tsbs_load_timescaledb \
    --host="10.0.0.10" \
    --port=5432 \
    --user="postgres" \
    --pass="postgres" \
    --seed=42 \
    --workers=$WORKERS \
    --batch-size=$BATCH_SIZE \
    --chunk-time=$CHUNK_TIME \
    --hash-workers=false \
    --file=$FILE \
    --results-file=$RESULTS_FILE
