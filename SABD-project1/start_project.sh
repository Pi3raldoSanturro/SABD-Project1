#! /bin/bash

# Project Start
echo "Project Start"

# Building HDFS image
echo "Building HDFS image"
echo ""
echo ""
./Scripts/hdfs_build.sh

# Starting Docker Compose
gnome-terminal --title="Docker Compose Bash" -- bash -c "docker compose up"
echo ""
echo ""

# Waiting for HDFS, Nifi, Spark, and Hbase Web UIs to be up
echo "Wait until HDFS, Nifi, Spark, and Hbase Web UIs are up"
echo "You can check the Web UIs at:
    - HDFS:     http://localhost:9870
    - Spark:    http://localhost:8080
    - Nifi:     http://localhost:8090
"
echo ""
echo ""

# Prompt to proceed when Nifi is fully up
echo "If Nifi is totally up, press ENTER"
read input
echo ""
echo ""

# Setting permissions for Nifi operation
echo "Nifi needs full permissions to operate well:"
sudo chmod -R 777 nifi/
sudo cp ./dataset/dataset.csv ./nifi/input_data
sudo chmod -R 777 nifi/
echo "Dataset successfully copied to Nifi container"

echo ""
echo ""

# Providing instructions for loading flow template in Nifi Web UI
echo "
Now you can load the flow template in the Nifi/ folder into Nifi Web UI. 
This template will take a CSV file from input, filter its columns, change its format to .parquet, and push it into HDFS. 
At the start of the compose file, an HDFS folder called 'dataset' with read and write permission is created inside HDFS, so Nifi will save the filtered and formatted dataset to that folder. 
When you launch the flow, check if everything is okay inside the HDFS cluster.
"

echo ""
echo ""

# Prompt to proceed with Spark execution
echo "Press ENTER if you want to go ahead with Spark"
read input

# Executing the Spark script
echo "Let's execute the Spark script"

# For additional safety for the flow of our project, let's change our dataset folder permissions inside our HDFS
echo "Changing permissions for HDFS new data"
docker exec master /bin/sh -c "hdfs dfs -chmod -R 777 /"

# Opening a new bash for the Spark exec
echo "Opening a new bash for the Spark exec:"
gnome-terminal --title="Spark bash" -- bash -c "./Scripts/spark_exec.sh && docker exec master /bin/sh -c 'hdfs dfs -chmod -R 777 /'; exec bash"

echo ""
echo ""

# Prompt to proceed after Spark execution for downloading result files with Nifi
echo "Now you can download the result files with Nifi. Just run the other blocks of the Nifi template. Press Enter when the downloads are ended"
read input
echo ""
echo ""

# Notification of the availability of result files in the Nifi/output_data directory
echo "You can now see all the result files in the nifi/output_data directory"
echo ""
echo ""

echo "Now if you want you can ingest the results in MongoDB with Apache Spark. Do you want to do that automatically? [y/n]"
echo "You can also go to /MongoDB folder and run ./spark_mongo_insert.sh script manually"
echo "For visualizing the results i installed MongoDB compass, a desktop viewer for my MongoDB Queries"
read input

if [ "$input" = "y" ]; then
    gnome-terminal --title="MongoDB bash" --wait -- bash -c "./MongoDB/spark_mongo_insert.sh; ; exec bash"
fi

echo ""
echo ""


# Prompt to stop the project and clear all containers
echo "Press ENTER to stop the project and clear all containers"
read input

# Stopping the project and clearing all containers
echo "Just Wait..."
./Scripts/remove_all.sh

echo ""
echo ""

# Project ended
#FINISHED

