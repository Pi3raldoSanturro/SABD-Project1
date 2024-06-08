from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, collect_set, count, concat_ws
from datetime import datetime

# Variabile per decidere se stampare o meno i risultati intermedi
print_intermediate = False

# Variabile per decidere se visualizzare o meno i DAG
visualize_dag = False

# Avvia il conteggio del tempo totale
start_total_time = datetime.now()

# Crea una sessione Spark
spark = SparkSession.builder \
    .appName("Query2_Dataframes") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Legge il dataset Parquet da HDFS
start_query_time = datetime.now()
df = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")
end_query_time = datetime.now()

# Calcola il tempo di esecuzione della query
query_execution_time = end_query_time - start_query_time

# Assicura che i campi siano nel formato corretto
df = df.withColumn("failure", col("failure.member0").cast("int"))
df = df.withColumn("vault_id", col("vault_id.member0").cast("int"))

# Query 1: Classifica dei modelli di hard disk con il maggior numero di fallimenti
start_query1_time = datetime.now()

# Calcola il numero totale di fallimenti per ogni modello di hard disk
failures_count = df.groupBy("model").agg(spark_sum(col("failure")).alias("failures_count"))

# Ordina i risultati in ordine decrescente e prendi solo i primi 10
sorted_failures = failures_count.orderBy(col("failures_count").desc()).limit(10)

if print_intermediate:
    print("Classifica dei modelli di hard disk con il maggior numero di fallimenti:")
    sorted_failures.show()

# Salva i risultati in HDFS in formato CSV nella directory /results2.1
sorted_failures.write.csv("hdfs://master:54310/results2.1/", header=True, mode="overwrite")

end_query1_time = datetime.now()
query1_execution_time = end_query1_time - start_query1_time

# Query 2: Classifica dei vault con il maggior numero di fallimenti e modelli unici
start_query2_time = datetime.now()

# Filtra solo i fallimenti
failures_df = df.filter(col("failure") == 1)

if print_intermediate:
    print("Dataset filtrato per fallimenti:")
    failures_df.show()

# Raggruppa per vault_id e aggrega i risultati
vault_failures = failures_df.groupBy("vault_id") \
    .agg(
        count("failure").alias("total_failures"),
        collect_set("model").alias("unique_models")
    )

# Converte l'array di modelli unici in una stringa separata da virgole
vault_failures = vault_failures.withColumn("unique_models", concat_ws(",", col("unique_models")))

if print_intermediate:
    print("Classifica dei vault con il maggior numero di fallimenti e modelli unici:")
    vault_failures.show()

# Ordina i risultati in base al numero totale di fallimenti in ordine decrescente e prendi i primi 10
sorted_vaults = vault_failures.orderBy(col("total_failures").desc()).limit(10)

if print_intermediate:
    print("Vault ordinati per numero totale di fallimenti:")
    sorted_vaults.show()

# Salva i risultati in HDFS in formato CSV nella directory /results2.2
sorted_vaults.write.csv("hdfs://master:54310/results2.2/", header=True, mode="overwrite")

end_query2_time = datetime.now()
query2_execution_time = end_query2_time - start_query2_time

# Calcola il tempo totale di esecuzione del programma
end_total_time = datetime.now()
total_execution_time = end_total_time - start_total_time

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
print("\nTEMPI PER LE QUERY\n")

# Stampa il tempo di esecuzione delle operazioni di query
print("Tempo di esecuzione per interrogare il dataset: {}".format(query_execution_time))
print("Tempo di esecuzione per la query 1: {}".format(query1_execution_time))
print("Tempo di esecuzione per la query 2: {}".format(query2_execution_time))

# Stampa il tempo totale di esecuzione del programma
print("Tempo totale di esecuzione del programma: {}".format(total_execution_time))

if visualize_dag:
    print("You can check the DAG at http:localhost:4040, press CTRL C for stopping the program and close the spark session:")
    input()

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")

# Ferma la sessione Spark
spark.stop()
