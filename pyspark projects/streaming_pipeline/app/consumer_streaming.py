from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Criação da SparkSession com suporte Kafka
spark = SparkSession.builder \
    .appName("StreamingTempoReal") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Leitura dos eventos do Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pedidos_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Conversão de bytes -> string
df_str = df_raw.selectExpr("CAST(value AS STRING)")

# Definição do schema do JSON
schema = StructType() \
    .add("pedido_id", IntegerType()) \
    .add("valor", DoubleType()) \
    .add("status", StringType())

# Parse JSON
df_parsed = df_str.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Processamento: contagem de pedidos por status
df_agg = df_parsed.groupBy("status").agg(count("*").alias("total_pedidos"))

# Escrita em tempo real no console
query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
