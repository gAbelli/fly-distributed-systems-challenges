#!/bin/bash

SCRIPT_DIR=$(pwd)/bin
mkdir -p $SCRIPT_DIR
go build -o $SCRIPT_DIR/main
"$MAELSTROM_PATH/maelstrom" test -w broadcast --bin $SCRIPT_DIR/main --node-count 25 --time-limit 20 --rate 100 --latency 100
