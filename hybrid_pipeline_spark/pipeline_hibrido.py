#Batch Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("HybridPipeline") \
    .getOrCreate()

# Lendo dados históricos
df_batch = spark.read.csv("s3://bucket/dados_historicos/", header=True, inferSchema=True)

# Transformação
df_agg = df_batch.groupBy("categoria").agg(sum("valor").alias("total_valor"))

# Salvando o resultado
df_agg.write.mode("overwrite").parquet("s3://bucket/resultados_batch/")


#Streaming Pipeline
from pyspark.sql.functions import expr

# Lendo dados em tempo real (Kafka)
df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topico_dados") \
    .load()

# Convertendo valor do Kafka para string
df_stream = df_stream.selectExpr("CAST(value AS STRING) as raw")

# Transformação simples
df_stream_transformed = df_stream.withColumn("categoria", expr("split(raw, ',')[0]")) \
                                 .withColumn("valor", expr("CAST(split(raw, ',')[1] AS double)"))

df_stream_agg = df_stream_transformed.groupBy("categoria").sum("valor")

# Salvando resultados incrementalmente
query = df_stream_agg.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("checkpointLocation", "s3://bucket/checkpoints/") \
    .option("path", "s3://bucket/resultados_streaming/") \
    .start()


#Unindo batch e streaming
from delta.tables import *

delta_table_path = "s3://bucket/resultados_delta/"

# Criar ou carregar Delta Table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Merge streaming updates no Delta Table
df_stream_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/") \
    .start(delta_table_path)
