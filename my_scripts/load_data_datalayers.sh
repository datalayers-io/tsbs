#!/bin/bash

SCENARIO=$1
case $SCENARIO in
  1)
    CONFIG_FILE="cpu-only-100-31d.yaml"
    ;;
  2)
    CONFIG_FILE="cpu-only-4000-4d.yaml"
    ;;
  3)
    CONFIG_FILE="cpu-only-100000-3h.yaml"
    ;;
  4)
    CONFIG_FILE="cpu-only-1000000-3m.yaml"
    ;;
  5)
    CONFIG_FILE="cpu-only-10000000-3m.yaml"
    ;;
  *)
    echo "Invalid SCENARIO: $SCENARIO"
    echo "SCENARIO options: 1, 2, 3, 4, 5"
    exit 1
    ;;
esac

# FIXME(niebayes): figure out why writing in some scenarios fails.
./bin/tsbs_load load datalayers --config=./load_config/datalayers/$CONFIG_FILE
