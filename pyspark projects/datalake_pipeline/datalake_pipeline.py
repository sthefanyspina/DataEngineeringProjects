# ========================================
# Projeto PySpark - Data Lake Bronze / Silver / Gold
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, sum, count
import logging
import os
import time

# -------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=f"logs/pipeline_datalake_{time.strftime('%Y%m%d_%H%M%S')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("=== Iniciando Pipeline de Data Lake ===")

# -------------------------
# CONFIGURAÇÃO DO SPARK
# -------------------------
spark = SparkSession.builder \
    .appName("DataLakePipeline_Bronze_Silver_Gold") \
    .getOrCreate()

# -------------------------
# ESTRUTURA DE PASTAS
# -------------------------
os.makedirs("data_lake/bronze", exist_ok=True)
os.makedirs("data_lake/silver", exist_ok=True)
os.makedirs("data_lake/gold", exist_ok=True)

# -------------------------
# 1️⃣ CAMADA BRONZE - Ingestão
# -------------------------
try:
    logging.info("Lendo dados brutos CSV...")
    df_bronze = spark.read.option("header", True).csv("input/vendas.csv")

    logging.info(f"Linhas lidas (Bronze): {df_bronze.count()}")
    df_bronze.write.mode("overwrite").parquet("data_lake/bronze/vendas_raw.parquet")
    logging.info("Camada Bronze salva com sucesso.")
except Exception as e:
    logging.error(f"Erro na camada Bronze: {str(e)}")

# -------------------------
# 2️⃣ CAMADA SILVER - Limpeza e Deduplicação
# -------------------------
try:
    logging.info("Processando camada Silver...")
    df_bronze = spark.read.parquet("data_lake/bronze/vendas_raw.parquet")

    df_silver = (
        df_bronze
        .withColumn("cliente", trim(lower(col("cliente"))))
        .withColumn("valor_venda", col("valor_venda").cast("double"))
    )

    # Deduplicar por ID
    if "id_venda" in df_silver.columns:
        df_silver = df_silver.dropDuplicates(["id_venda"])

    logging.info(f"Linhas após limpeza (Silver): {df_silver.count()}")

    df_silver.write.mode("overwrite").parquet("data_lake/silver/vendas_clean.parquet")
    logging.info("Camada Silver salva com sucesso.")
except Exception as e:
    logging.error(f"Erro na camada Silver: {str(e)}")

# -------------------------
# 3️⃣ CAMADA GOLD - Enriquecimento e Agregação
# -------------------------
try:
    logging.info("Processando camada Gold...")
    df_silver = spark.read.parquet("data_lake/silver/vendas_clean.parquet")

    df_gold = (
        df_silver.groupBy("cliente")
        .agg(
            sum("valor_venda").alias("total_vendas"),
            count("*").alias("qtd_compras")
        )
    )

    logging.info(f"Linhas finais (Gold): {df_gold.count()}")
    df_gold.write.mode("overwrite").parquet("data_lake/gold/vendas_aggregated.parquet")
    logging.info("Camada Gold salva com sucesso.")
except Exception as e:
    logging.error(f"Erro na camada Gold: {str(e)}")

# -------------------------
# FINALIZAÇÃO
# -------------------------
logging.info("=== Pipeline concluído com sucesso! ===")
spark.stop()
