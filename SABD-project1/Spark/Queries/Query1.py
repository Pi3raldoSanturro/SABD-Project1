from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime

# Variabile per decidere se stampare o meno i risultati intermedi
print_intermediate = False

# Variabile per decidere se visualizzare o meno i DAG
visualize_dag = False

# Avvia il conteggio del tempo totale
start_total_time = datetime.now()

# Crea una sessione Spark
spark = SparkSession.builder \
    .appName("Query1_Dataframes") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Legge il dataset Parquet da HDFS
start_query_time = datetime.now()
df = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")
end_query_time = datetime.now()

# Calcola il tempo di esecuzione della query
query_execution_time = end_query_time - start_query_time

# Converte le colonne in interi
df = df.withColumn("failure", col("failure.member0").cast(IntegerType()))
df = df.withColumn("vault_id", col("vault_id.member0").cast(IntegerType()))

# Filtra solo i failure
failures_df = df.filter(col("failure") == 1)

# Aggiunge una colonna con la data in formato "dd-MM-yyyy"
@udf(StringType())
def format_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f').strftime('%d-%m-%Y')

failures_df = failures_df.withColumn("date", format_date(col("date").cast("string")))

# Conta i failure per ogni giorno e per ogni vault
failures_count_df = failures_df.groupBy("date", "vault_id").count()
failures_count_df = failures_count_df.withColumnRenamed("count", "failure_count")

if print_intermediate:
    print("Conto dei failure per ogni giorno e per ogni vault:")
    failures_count_df.show()

# Filtra i vault con esattamente 2, 3 o 4 failure
filtered_df = failures_count_df.filter(col("failure_count").isin(2, 3, 4))

if print_intermediate:
    print("Vault filtrati con 2, 3 o 4 failure:")
    filtered_df.show()

# Ordina i risultati
sorted_df = filtered_df.orderBy("date", "vault_id")

if print_intermediate:
    print("Risultati ordinati:")
    sorted_df.show()

# Avvia il conteggio del tempo di salvataggio
start_save_time = datetime.now()

# Salva i risultati in HDFS in formato CSV
sorted_df.coalesce(1).write.csv("hdfs://master:54310/results1/", header=True, mode="overwrite")

# Termina il conteggio del tempo di salvataggio
end_save_time = datetime.now()

# Calcola il tempo di esecuzione del salvataggio
save_execution_time = end_save_time - start_save_time

# Calcola il tempo totale di esecuzione del programma
end_total_time = datetime.now()
total_execution_time = end_total_time - start_total_time

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
print("\nTEMPI PER LA PRIMA QUERY\n")

# Stampa il tempo di esecuzione delle operazioni di query
print("Tempo di esecuzione delle operazioni di query: {}".format(query_execution_time))

# Stampa il tempo di esecuzione del salvataggio
print("Tempo di esecuzione del salvataggio: {}".format(save_execution_time))

# Stampa il tempo totale di esecuzione del programma
print("Tempo totale di esecuzione del programma: {}".format(total_execution_time))

if visualize_dag:
    print("You can check the DAG at http:localhost:4040, press CTRL C for stopping the program and close the spark session:")
    input()

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")



# Ferma la sessione Spark
spark.stop()
