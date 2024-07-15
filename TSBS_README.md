## Compile
``` shell
make
```
to compile all necessary binaries.

## Generate benchmark data
tsbs_generate_data \
    --use-case="cpu-only" \
    --seed=42 \
    --scale=100 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-01T06:00:00Z" \
    --log-interval="10s" \
    --format="datalayers" \
    --file ./datalayers_cpu-only.data

## Load data
tsbs_load load \
    --data-source.type="FILE" \
    --data-source.file.location="./datalayers_cpu-only.data" \
    --loader.runner.db-name="benchmark" \
    --loader.runner.db-create-db=true \
    --loader.runner.db-abort-on-exist=true \
    --loader.runner.do-load=true \
    --loader.runner.workers=8 \
    --loader.runner.hash-workers=true \
    --loader.runner.flow-control=true \
    --loader.runner.batch-size=5000 \
    --loader.runner.seed=42 \
    --loader.runner.reporting-period=5s \
    --loader.runner.results-file="./cpu-only-result.json \
    --loader.db-specific.sql-endpoint="127.0.0.1:8360"

## Generate queries
TODO

## Run queries
