from pyspark.sql import SparkSession

def write_csv_to_mongodb(hdfs_folder, db_name, collection_name):
    # Crea una sessione Spark con il connettore MongoDB incluso come dipendenza
    spark = SparkSession.builder \
        .appName("HDFS to MongoDB") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    # Leggi i dati dai file CSV nella cartella specificata su HDFS
    df = spark.read.csv(hdfs_folder, header=True, inferSchema=True)

    # Scrivi i dati in MongoDB
    df.write.format("com.mongodb.spark.sql.DefaultSource") \
           .option("uri", f"mongodb://mongodb:27017/{db_name}.{collection_name}") \
           .mode("append") \
           .save()

    # Ferma la sessione Spark
    spark.stop()


write_csv_to_mongodb("hdfs://master:54310/results1/", "Query1_Dataframes", "Results")
write_csv_to_mongodb("hdfs://master:54310/results2.1/", "Query2_1_Dataframes", "Results")
write_csv_to_mongodb("hdfs://master:54310/results2.2/", "Query2_2_Dataframes", "Results")
write_csv_to_mongodb("hdfs://master:54310/results3/", "Query3_Dataframes", "Results")
write_csv_to_mongodb("hdfs://master:54310/results1_SQL/", "Query1_SQL", "Results")
write_csv_to_mongodb("hdfs://master:54310/results2.1_SQL/", "Query2_1_SQL", "Results")
write_csv_to_mongodb("hdfs://master:54310/results2.2_SQL/", "Query2_2_SQL", "Results")
write_csv_to_mongodb("hdfs://master:54310/results3_SQL/", "Query3_SQL", "Results")


