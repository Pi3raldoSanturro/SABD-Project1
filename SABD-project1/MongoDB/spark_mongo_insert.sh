#!/bin/bash

docker cp ./insert_mongo.py spark-master:/tmp/insert_mongo.py

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores 1 \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
    /tmp/insert_mongo.py

