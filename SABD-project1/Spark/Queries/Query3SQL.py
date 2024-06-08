from pyspark.sql import SparkSession
import time
from datetime import datetime

# Variabile per decidere se stampare o meno i risultati intermedi
print_intermediate = False

# Variabile per decidere se visualizzare o meno i DAG
visualize_dag = False


# Start total time counting
start_total_time = datetime.now()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Query3_SQL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Start timer for query processing
start_time = time.time()

# Load data from Parquet file
data = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")
data.createOrReplaceTempView("dataset")

# Ensure fields are in correct format
query = """
SELECT
    *,
    CAST(failure.member0 AS INT) AS failures
FROM dataset
"""
formatted_data = spark.sql(query)
formatted_data.createOrReplaceTempView("formatted_dataset")

# Define a partition window by serial_number ordered by date descending and add a row number column
query = """
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY serial_number ORDER BY date DESC) AS rank
FROM formatted_dataset
"""
data_with_rank = spark.sql(query)
data_with_rank.createOrReplaceTempView("data_with_rank")

if print_intermediate:
    print("Query3SQL colonna row number aggiunta:")
    sorted_failures.show()


# Filter to keep only rows with rank 1 (latest observation for each hard disk)
query = """
SELECT
    *
FROM data_with_rank
WHERE rank = 1
"""
latest_data = spark.sql(query)
latest_data.createOrReplaceTempView("latest_data")

# Filter data for failure and non-failure
query = """
SELECT *
FROM latest_data
WHERE failures = 1
"""
failure_data = spark.sql(query)
failure_data.createOrReplaceTempView("failure_data")

query = """
SELECT *
FROM latest_data
WHERE failures = 0
"""
no_failure_data = spark.sql(query)
no_failure_data.createOrReplaceTempView("no_failure_data")

if print_intermediate:
    print("Query3SQL filtraggio:")
    sorted_failures.show()

# Function to calculate statistics for a given DataFrame
def calculate_statistics(table_name, failure_flag):
    query = f"""
    SELECT
        {failure_flag} AS failure,
        MIN(s9_power_on_hours.member0) AS min,
        PERCENTILE_APPROX(s9_power_on_hours.member0, 0.25) AS 25th_percentile,
        PERCENTILE_APPROX(s9_power_on_hours.member0, 0.5) AS 50th_percentile,
        PERCENTILE_APPROX(s9_power_on_hours.member0, 0.75) AS 75th_percentile,
        MAX(s9_power_on_hours.member0) AS max,
        COUNT(s9_power_on_hours.member0) AS count
    FROM {table_name}
    """
    stats = spark.sql(query).first()
    
    statistics_list = [
        failure_flag,
        stats["min"],
        stats["25th_percentile"],
        stats["50th_percentile"],
        stats["75th_percentile"],
        stats["max"],
        stats["count"]
    ]
    
    return statistics_list

# Calculate statistics for failure and non-failure data
failure_stats = calculate_statistics("failure_data", 1)
no_failure_stats = calculate_statistics("no_failure_data", 0)

# Create DataFrame with the calculated statistics
columns = ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"]
results = spark.createDataFrame([failure_stats, no_failure_stats], columns)

if print_intermediate:
    print("Query3SQL risultato:")
    sorted_failures.show()

# Start saving time counting
start_save_time = time.time()

# Save the results in CSV format
results.coalesce(1).write.csv("hdfs://master:54310/results3_SQL/", header=True, mode="overwrite")

# End saving time counting
end_save_time = time.time()

# Calculate time taken for saving
save_execution_time = end_save_time - start_save_time

# Calculate and print query processing times
end_time = time.time()
processing_time = end_time - start_time
print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
print("\nTIMES FOR QUERY 3 SQL\n")
print("Execution time for query processing: {} seconds".format(processing_time))
print("Execution time for saving: {} seconds".format(save_execution_time))

# Calculate total program execution time
end_total_time = datetime.now()
total_execution_time = end_total_time - start_total_time
print("Total program execution time: {}".format(total_execution_time))

if visualize_dag:
    print("You can check the DAG at http:localhost:4040, press CTRL C for stopping the program and close the spark session:")
    input()

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")

# Stop Spark session
spark.stop()
