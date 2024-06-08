#!/bin/bash
: ${HADOOP_PREFIX:=/usr/local/hadoop}
sudo $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid
service ssh start
# $HADOOP_PREFIX/sbin/start-dfs.sh

hdfs namenode -format

$HADOOP_HOME/sbin/start-all.sh

hdfs dfs -mkdir /dataset
hdfs dfs -chmod 777
hdfs dfs -chmod -R 0777 /

echo "Hadoop cluster succesfully started"

# Launch bash console  
/bin/bash
