"""
Passaggi dettagliati:
1. Inizializzazione della sessione Spark.
2. Lettura dei dati dal file Parquet.
3. Conversione dei campi in formato corretto.
4. Definizione di una finestra di partizione per ogni serial_number ordinata per data decrescente.
5. Aggiunta di una colonna con il numero di riga per ogni partizione.
6. Filtraggio per mantenere solo le righe con rank 1 (ultima osservazione per ogni hard disk).
7. Filtraggio dei dati per fallimenti e non fallimenti.
8. Calcolo delle statistiche per i dati di fallimento e non fallimento.
9. Salvataggio dei risultati in formato CSV su HDFS.
10. Stampa dei tempi di esecuzione delle varie fasi.
11. Arresto della sessione Spark.

La variabile `print_intermediate` permette di decidere se stampare o meno i risultati intermedi durante l'esecuzione.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, count, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import time
from datetime import datetime

# Variabile per decidere se stampare o meno i risultati intermedi
print_intermediate = False

# Variabile per decidere se visualizzare o meno i DAG
visualize_dag = False

# Avvia il conteggio del tempo totale
start_total_time = datetime.now()

# Inizializza la sessione Spark
spark = SparkSession.builder \
    .appName("Query3_Dataframes") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Avvia il timer per l'elaborazione della query
start_time = time.time()

# Carica i dati dal file Parquet
start_read_time = time.time()
data = spark.read.parquet("hdfs://master:54310/dataset/dataset.parquet")
end_read_time = time.time()

# Calcola il tempo di esecuzione per la lettura
read_execution_time = end_read_time - start_read_time

# Assicura che i campi siano nel formato corretto
data = data.withColumn("failures", col("failure.member0").cast("int"))

# Definisci una finestra di partizione per serial_number ordinata per data in ordine decrescente

window_spec = Window.partitionBy("serial_number").orderBy(col("date").desc())

# L'API Window viene utilizzata per creare una finestra di partizione che permette di eseguire funzioni di finestra 
# (come row_number, rank, dense_rank) su partizioni dei dati.
# In questo caso, partitionBy("serial_number") suddivide i dati in partizioni per ogni "serial_number" 
# e orderBy(col("date").desc()) ordina ogni partizione per "date" in ordine decrescente.
# Questo consente di aggiungere un numero di riga unico a ogni riga all'interno della partizione ordinata, 
# utile per operazioni come trovare l'ultima osservazione per ogni hard disk.
# Maggiori dettagli sull'uso dell'API Window sono disponibili qui: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html

# Aggiungi una colonna con il numero di riga per ogni partizione
data_with_rank = data.withColumn("rank", row_number().over(window_spec))

if print_intermediate:
    print("Dataset con colonna 'rank' aggiunta:")
    data_with_rank.show()

# Filtra per mantenere solo le righe con rank 1 (ultima osservazione per ogni hard disk)
latest_data = data_with_rank.filter(col("rank") == 1).drop("rank")

if print_intermediate:
    print("Ultima osservazione per ogni hard disk:")
    latest_data.show()

# Filtra i dati per fallimenti e non fallimenti
failure_data = latest_data.filter(col("failures") == 1)
no_failure_data = latest_data.filter(col("failures") == 0)

if print_intermediate:
    print("Dati di fallimento:")
    failure_data.show()
    print("Dati senza fallimento:")
    no_failure_data.show()

# Funzione per calcolare le statistiche per un DataFrame dato
def calculate_statistics(df, failure_flag):
    # Aggrega le statistiche per il DataFrame
    stats = df.agg(
        min("s9_power_on_hours.member0").alias("min"),
        expr("percentile_approx(s9_power_on_hours.member0, 0.25)").alias("25th_percentile"),
        expr("percentile_approx(s9_power_on_hours.member0, 0.5)").alias("50th_percentile"),
        expr("percentile_approx(s9_power_on_hours.member0, 0.75)").alias("75th_percentile"),
        max("s9_power_on_hours.member0").alias("max"),
        count("s9_power_on_hours.member0").alias("count")
    ).first()
    
# percentile_approx è una funzione che calcola il percentile approssimativo di una colonna numerica.
# È particolarmente utile per dataset molto grandi dove un calcolo esatto del percentile sarebbe troppo costoso.
# La funzione prende due parametri: il nome della colonna e il percentile desiderato (tra 0 e 1).
# Maggiori dettagli su percentile_approx sono disponibili qui: https://spark.apache.org/docs/latest/api/sql/index.html#percentile_approx

    # Crea una lista contenente le statistiche calcolate
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

# Calcola le statistiche per i dati di fallimento e non fallimento
failure_stats = calculate_statistics(failure_data, 1)
no_failure_stats = calculate_statistics(no_failure_data, 0)

# Crea un DataFrame con i risultati
columns = ["failure", "min", "25th_percentile", "50th_percentile", "75th_percentile", "max", "count"]
results = spark.createDataFrame([failure_stats, no_failure_stats], columns)

if print_intermediate:
    print("Statistiche calcolate:")
    results.show()

# Avvia il conteggio del tempo di salvataggio
start_save_time = time.time()

# Salva i risultati in formato CSV
results.coalesce(1).write.csv("hdfs://master:54310/results3/", header=True, mode="overwrite")

# Termina il conteggio del tempo di salvataggio
end_save_time = time.time()

# Calcola il tempo di esecuzione per il salvataggio
save_execution_time = end_save_time - start_save_time

# Calcola e stampa i tempi di elaborazione delle query
end_time = time.time()
processing_time = end_time - start_time
print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
print("\nTEMPI PER LA QUERY 3\n")
print("Tempo di esecuzione per l'elaborazione della query: {} secondi".format(processing_time))
print("Tempo di esecuzione per la lettura: {} secondi".format(read_execution_time))
print("Tempo di esecuzione per il salvataggio: {} secondi".format(save_execution_time))

# Calcola il tempo totale di esecuzione del programma
end_total_time = datetime.now()
total_execution_time = end_total_time - start_total_time
print("Tempo totale di esecuzione del programma: {}".format(total_execution_time))

if visualize_dag:
    print("You can check the DAG at http:localhost:4040, press CTRL C for stopping the program and close the spark session:")
    input()

print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")



# Ferma la sessione Spark
spark.stop()
