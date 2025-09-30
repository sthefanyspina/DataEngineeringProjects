# pipeline_batch.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit

# -----------------------------
# Configuração de logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# -----------------------------
# Criar Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("BatchIngestionPipeline") \
    .getOrCreate()

logging.info("Spark session criada.")

# -----------------------------
# Ler dados (Extract)
# -----------------------------
input_path = "dados/vendas.csv"

raw_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(input_path)

logging.info(f"Linhas lidas: {raw_df.count()}")

# -----------------------------
# Transformar dados (Transform)
# -----------------------------
clean_df = raw_df \
    .dropna(subset=["Order_id", "Price", "Quantity"]) \
    .withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd")) \
    .withColumn("Revenue", col("Price") * col("Quantity")) \
    .dropDuplicates(["Order_id"]) \
    .withColumn("Batch_Date", lit("2025-09-28"))  # Data do processamento

logging.info(f"Linhas após limpeza: {clean_df.count()}")

# -----------------------------
# Carregar dados (Load)
# -----------------------------
output_path = "output/vendas_parquet"

clean_df.write \
    .mode("overwrite") \
    .partitionBy("Batch_Date") \
    .parquet(output_path)

logging.info(f"Dados salvos em {output_path}")

# -----------------------------
# Finalizar Spark
# -----------------------------
spark.stop()
logging.info("Pipeline finalizado com sucesso!")
