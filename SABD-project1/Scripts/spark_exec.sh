#!/bin/bash

# This script copies the Spark query files into the Spark container for execution.
# It's assumed that the Spark container is running and named 'spark-master'.
# You can change the NUM_COR variable to change the number of executor cores. MAX 3

NUM_COR=3 

# Copy the query files into the Spark container
docker cp ./Spark/Queries/Query1.py spark-master:/tmp/Query1.py
docker cp ./Spark/Queries/Query1_SQL.py spark-master:/tmp/Query1_SQL.py
docker cp ./Spark/Queries/Query2.py spark-master:/tmp/Query2.py
docker cp ./Spark/Queries/Query2SQL.py spark-master:/tmp/Query2SQL.py
docker cp ./Spark/Queries/Query3.py spark-master:/tmp/Query3.py
docker cp ./Spark/Queries/Query3SQL.py spark-master:/tmp/Query3SQL.py



# Spark submit commands for executing the queries
# Note: Adjust the resource configurations (--executor-memory and --total-executor-cores) as needed

# # Execute Query1.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores $NUM_COR \
    /tmp/Query1.py

# # Execute Query1_SQL.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores $NUM_COR \
    /tmp/Query1_SQL.py

# # Execute Query2.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores $NUM_COR \
    /tmp/Query2.py

# # Execute Query2SQL.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores $NUM_COR \
    /tmp/Query2SQL.py

# # Execute Query3.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores $NUM_COR \
    /tmp/Query3.py

# Execute Query3SQL.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.defaultFS=hdfs://master:54310 \
    --executor-memory 512m \
    --total-executor-cores $NUM_COR \
    /tmp/Query3SQL.py

