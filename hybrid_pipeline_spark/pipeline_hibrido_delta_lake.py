from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HybridPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from pyspark.sql.functions import col, sum
from delta.tables import DeltaTable

# Caminho para dados históricos CSV
historical_path = "s3://bucket/dados_historicos/"

# Lendo dados históricos
df_batch = spark.read.csv(historical_path, header=True, inferSchema=True)

# Transformação: agregando por categoria
df_batch_agg = df_batch.groupBy("categoria").agg(sum("valor").alias("total_valor"))

# Salvando no Delta Lake
delta_path = "s3://bucket/delta_resultados/"

df_batch_agg.write.format("delta").mode("overwrite").save(delta_path)


# Lendo dados do Kafka
df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topico_dados") \
    .load()

# Convertendo valor para string e extraindo colunas
from pyspark.sql.functions import expr

df_stream_transformed = df_stream.selectExpr("CAST(value AS STRING) as raw") \
    .withColumn("categoria", expr("split(raw, ',')[0]")) \
    .withColumn("valor", expr("CAST(split(raw, ',')[1] AS double)"))


# Carregando Delta Table
delta_table = DeltaTable.forPath(spark, delta_path)

# Função de merge para atualizar Delta com novos dados do streaming
from delta.tables import *

def upsert_to_delta(microBatchDF, batchId):
    delta_table.alias("target").merge(
        microBatchDF.alias("source"),
        "target.categoria = source.categoria"
    ).whenMatchedUpdate(
        set={"total_valor": "target.total_valor + source.valor"}
    ).whenNotMatchedInsert(
        values={"categoria": "source.categoria", "total_valor": "source.valor"}
    ).execute()

# Aplicando merge em micro-batches
df_stream_transformed.writeStream \
    .foreachBatch(upsert_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", "s3://bucket/checkpoints/") \
    .start()

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS resultados
    USING DELTA
    LOCATION '{delta_path}'
""")

# Exemplo de consulta agregada
spark.sql("SELECT categoria, total_valor FROM resultados ORDER BY total_valor DESC").show()
