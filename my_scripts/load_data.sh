#!/bin/bash

FORMAT=$1
USE_CASE=$2

./bin/tsbs_load load $FORMAT --config=./load_config/$FORMAT/$USE_CASE.yaml
