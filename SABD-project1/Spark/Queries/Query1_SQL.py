from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from datetime import datetime

# Variabile per decidere se stampare o meno i risultati intermedi
print_intermediate = False

# Start total time counting
start_total_time = datetime.now()

# Create Spark session
spark = SparkSession.builder \
    .appName("Query1_SQL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read Parquet dataset from HDFS (Azione 1)
start_query_time = datetime.now()
df = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")
df.createOrReplaceTempView("dataset")
end_query_time = datetime.now()

# Calculate time taken for querying the dataset
query_execution_time = end_query_time - start_query_time

# SQL query to convert columns to integers and filter only failures
query = """
SELECT
    CAST(failure.member0 AS INT) AS failure,
    CAST(vault_id.member0 AS INT) AS vault_id,
    date
FROM dataset
WHERE CAST(failure.member0 AS INT) = 1
"""
failures_df = spark.sql(query)
failures_df.createOrReplaceTempView("failures")

# UDF to format date
spark.udf.register("format_date", lambda date_str: datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f').strftime('%d-%m-%Y'), StringType())

# SQL query to add formatted date column
query = """
SELECT
    failure,
    vault_id,
    format_date(CAST(date AS STRING)) AS date
FROM failures
"""
formatted_failures_df = spark.sql(query)
formatted_failures_df.createOrReplaceTempView("formatted_failures")

# SQL query to count failures for each day and each vault
query = """
SELECT
    date,
    vault_id,
    COUNT(*) AS failure_count
FROM formatted_failures
GROUP BY date, vault_id
HAVING failure_count IN (2, 3, 4)
"""
filtered_df = spark.sql(query)
filtered_df.createOrReplaceTempView("filtered_failures")

if print_intermediate:
    print("Conteggio fallimenti per ogni giorno e ogni vault:")
    filtered_df.show()


# SQL query to sort the results
query = """
SELECT
    date,
    vault_id,
    failure_count
FROM filtered_failures
ORDER BY date, vault_id
"""
sorted_df = spark.sql(query)

if print_intermediate:
    print("Query1 SQL ordinata:")
    sorted_df.show()

# Start saving time counting
start_save_time = datetime.now()

# Save results to HDFS in CSV format in directory /results (Azione 2)
sorted_df.coalesce(1).write.csv("hdfs://master:54310/results1_SQL/", header=True, mode="overwrite")

# End saving time counting
end_save_time = datetime.now()

# Calculate time taken for saving
save_execution_time = end_save_time - start_save_time

# Calculate total program execution time
end_total_time = datetime.now()
total_execution_time = end_total_time - start_total_time

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
print("\nTIMES FOR FIRST SQL QUERY\n")

# Print execution time for query operations
print("Execution time for query operations: {}".format(query_execution_time))

# Print execution time for saving
print("Execution time for saving: {}".format(save_execution_time))

# Print total program execution time
print("Total program execution time: {}".format(total_execution_time))

# print("Now you can check the DAG at http:localhost:4040, press ANY KEY for ending the program\n")
# input()

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")

# Stop Spark session
spark.stop()
