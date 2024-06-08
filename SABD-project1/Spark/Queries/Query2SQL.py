from pyspark.sql import SparkSession
from datetime import datetime

# Variabile per decidere se stampare o meno i risultati intermedi
print_intermediate = False

# Variabile per decidere se visualizzare o meno i DAG
visualize_dag = False


# Start total time counting
start_total_time = datetime.now()

# Create Spark session
spark = SparkSession.builder \
    .appName("Query2_SQL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read Parquet dataset from HDFS (Azione 1)
start_query_time = datetime.now()
df = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")
df.createOrReplaceTempView("dataset")
end_query_time = datetime.now()

# Calculate time taken for querying the dataset
query_execution_time = end_query_time - start_query_time

# Ensure fields are in correct format (Trasformazioni)
query = """
SELECT
    CAST(failure.member0 AS INT) AS failure,
    CAST(vault_id.member0 AS INT) AS vault_id,
    model
FROM dataset
"""
df = spark.sql(query)
df.createOrReplaceTempView("formatted_dataset")

# Query 1: Classifica dei modelli di hard disk con il maggior numero di fallimenti
start_query1_time = datetime.now()

query = """
SELECT
    model,
    SUM(failure) AS failures_count
FROM formatted_dataset
GROUP BY model
ORDER BY failures_count DESC
LIMIT 10
"""
sorted_failures = spark.sql(query)

if print_intermediate:
    print("Query2SQL Query1:")
    sorted_failures.show()


# Save the results to HDFS in CSV format in the directory /results2.1 (Azione 2)
sorted_failures.write.csv("hdfs://master:54310/results2.1_SQL/", header=True, mode="overwrite")

end_query1_time = datetime.now()
query1_execution_time = end_query1_time - start_query1_time

# Query 2: Classifica dei vault con il maggior numero di fallimenti e modelli unici
start_query2_time = datetime.now()

query = """
SELECT
    vault_id,
    COUNT(*) AS total_failures,
    COLLECT_SET(model) AS unique_models
FROM formatted_dataset
WHERE failure = 1
GROUP BY vault_id
"""
vault_failures = spark.sql(query)
vault_failures.createOrReplaceTempView("vault_failures")

query = """
SELECT
    vault_id,
    total_failures,
    CONCAT_WS(',', unique_models) AS unique_models
FROM vault_failures
ORDER BY total_failures DESC
LIMIT 10
"""
sorted_vaults = spark.sql(query)

if print_intermediate:
    print("Query2SQL sorted vaults:")
    sorted_vaults.show()


# Save the results to HDFS in CSV format in the directory /results2.2 (Azione 3)
sorted_vaults.write.csv("hdfs://master:54310/results2.2_SQL/", header=True, mode="overwrite")

end_query2_time = datetime.now()
query2_execution_time = end_query2_time - start_query2_time

# Calculate total program execution time
end_total_time = datetime.now()
total_execution_time = end_total_time - start_total_time

# Print execution times
print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
print("\nTIMES FOR QUERIES\n")

# Print execution time for query operations
print("Execution time for querying dataset: {}".format(query_execution_time))
print("Execution time for query 1: {}".format(query1_execution_time))
print("Execution time for query 2: {}".format(query2_execution_time))

# Print total program execution time
print("Total program execution time: {}".format(total_execution_time))

if visualize_dag:
    print("You can check the DAG at http:localhost:4040, press CTRL C for stopping the program and close the spark session:")
    input()

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")

# Stop Spark session (Azione 6)
spark.stop()
