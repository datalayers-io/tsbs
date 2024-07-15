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
### Generate load config (Optional if you have a config file)
tsbs_load config  --data-source="FILE" --target="datalayers"

### Perform loading
tsbs_load load datalayers --config=./config.yaml

## Generate queries
TODO

## Run queries
