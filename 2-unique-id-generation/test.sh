#!/bin/bash

SCRIPT_DIR=$(pwd)/bin
mkdir -p $SCRIPT_DIR
go build -o $SCRIPT_DIR/main
"$MAELSTROM_PATH/maelstrom" test -w unique-ids --bin $SCRIPT_DIR/main --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
