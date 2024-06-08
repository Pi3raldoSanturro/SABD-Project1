# Project Execution Instructions

To run the project, follow the steps below:

## 1. Dataset Preparation
Ensure that your dataset is named `dataset.csv` and place it in the `dataset` folder.

## 2. Start Project Script
Run the main script `start_project.sh` to initiate the project.

**Note:** Ensure that necessary permissions are granted for certain folders for smooth interaction with the frameworks.

**Note:** Multiple terminals will be opened during execution; do not close them.

## 3. Folder Details and general info about the architecture:
- **hdfs:** Contains elements required for a Docker build for a master node and slave nodes. Upon starting the master node, folders will be created in the cluster, and unlimited access permissions will be provided.

- **nifi:** Contains volumes for constructing and interacting with the Nifi node used in the project. The `input_data` folder will have the dataset in CSV format, serving as the input volume for Nifi, while the `output_data` folder will serve the opposite purpose. It contains an XML file representing the Nifi flow build, facilitating data ingestion between network frameworks. Manually insert the XML file as a template for Nifi.

**Note:** Nifi should never be shut down in this project, as the template contains sequences needed later.

**Note:** For proper functioning, it's necessary to operate Nifi template groups one at a time.

**Note:** The Nifi template consists of multiple groups; the main group manages data ingestion from local to HDFS, converts the dataset to Parquet format, and filters irrelevant columns. The other groups handle downloading Spark query results locally.

- **Prova:** Contains Python test scripts used to verify the correctness of query results. These scripts, although not mandatory for project execution, require the Pandas library to be installed via the command `pip install pandas`. If unsuccessful, alternative flags suggested by the terminal can be used.

- **Results:** Contains project results.

- **scripts:** Contains three scripts:
    - Builder for the HDFS image.
    - Script for executing Spark queries.
    - Script for removing all Docker elements upon project closure.

**Note:** Spark queries are executed by preloading a 'tmp' volume, which is then invoked via Spark at runtime.
**Note:** You can change the number of cores of the spark execution by modifying the NUM_COR variable

- **Spark:** Contains Python code for executing queries. In all the python scripts, you have two variables that you can enable to show intermidiate results of the operations done on dataset/sql, and a variable that you can enable to show Spark DAGs on localhost:4040 web page; both of that variables are disabled by default, you can go in /Spark/Queries/name_of_the_query.py and modify that variables.

- **MongoDB:** Contains the main python script that writes the results on MongoDB. For visualizing the Queries on MongoDB i used MongoDB compass, a user friendly desktop ui for Mongo: https://downloads.mongodb.com/compass/mongodb-mongosh_2.2.6_amd64.deb

## 4. Results

Downloading results are visible in the `nifi/nifi_output` folder. The project results are in `Results` folder.



At the end of the project execution, the main script will prompt you to terminate the entire Docker network built.

