from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Criar SparkSession
spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

# 2. Definir esquema dos dados que chegam
schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# 3. Ler dados do Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformar o valor do Kafka (bytes -> string -> JSON)
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. Aplicar transformações
df_transformed = df_json.filter(col("event_type") == "purchase")

# 6. Escrever para um sink (ex: Parquet no HDFS)
query = df_transformed.writeStream \
    .format("parquet") \
    .option("path", "/data/stream_output") \
    .option("checkpointLocation", "/data/checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
