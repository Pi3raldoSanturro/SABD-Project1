#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

MASTER_DIR="$SCRIPT_DIR/../hdfs/master"
SLAVE_DIR="$SCRIPT_DIR/../hdfs/slave"

docker build -t hadoop3-master "$MASTER_DIR"
docker build -t hadoop3-slave "$SLAVE_DIR"

