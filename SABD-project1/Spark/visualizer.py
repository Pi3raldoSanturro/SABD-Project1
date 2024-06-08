from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format
import time

# Creare la sessione Spark
spark = SparkSession.builder \
    .appName("FailureCountByVaultDF") \
    .getOrCreate()

# Inizio del timer
start_time = time.time()

# Leggere il dataset Parquet da HDFS
df = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")

# Filtrare solo i fallimenti
failures_df = df.filter(col("failure") == 1)

# Contare i fallimenti per ogni giorno e per ogni vault
failures_count_df = failures_df.groupBy(date_format(col("date"), "yyyy-MM-dd").alias("date"), "vault_id") \
    .agg(count("*").alias("failure_count"))

# Filtrare i vault con esattamente 4, 3, 2 fallimenti
filtered_df = failures_count_df.filter(col("failure_count").isin(2, 3, 4))

# Ordinare i risultati
sorted_df = filtered_df.orderBy("date", "vault_id")

# Salvare i risultati su HDFS in formato CSV
sorted_df.write.csv("hdfs://master:54310/results/query_1_results", header=True)

# Salvare i risultati su PC locale in formato CSV
sorted_df.coalesce(1).write.csv("/path/to/local/folder/query_1_results", header=True)

# Fine del timer
end_time = time.time()
processing_time = end_time - start_time
print(f"Tempo di processamento: {processing_time} secondi")

# Configurare la connessione ad HBase
catalog = ''.join("""{
    "table":{"namespace":"default", "name":"results"},
    "rowkey":"key",
    "columns":{
        "date":{"cf":"rowkey", "col":"date", "type":"string"},
        "vault_id":{"cf":"rowkey", "col":"vault_id", "type":"string"},
        "failure_count":{"cf":"details", "col":"failure_count", "type":"int"}
    }
}""".split())

# Aggiungere una colonna di chiave primaria
sorted_df = sorted_df.withColumn("key", col("date") + "_" + col("vault_id"))

# Scrivere su HBase
sorted_df.write \
    .options(catalog=catalog, newtable=5) \
    .format("org.apache.spark.sql.execution.datasources.hbase") \
    .save()

# Fermare la sessione Spark
spark.stop()

