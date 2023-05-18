#!/bin/bash

SCRIPT_DIR=$(pwd)/bin
mkdir -p $SCRIPT_DIR
go build -o $SCRIPT_DIR/main
"$MAELSTROM_PATH/maelstrom" test -w kafka --bin $SCRIPT_DIR/main --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
