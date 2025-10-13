# ========================================
# main_batch_pipeline.py
# Pipeline de Ingestão Batch PySpark + Great Expectations + Data Lake + Relatórios
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging
import os
import great_expectations as ge
import pandas as pd

# -------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("==== INÍCIO DO PIPELINE ====")

# -------------------------
# CRIAR SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("PipelineIngestaoBatch") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# -------------------------
# DEFINIR ESQUEMA
# -------------------------
schema_clientes = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("renda", DoubleType(), True)
])

schema_pedidos = StructType([
    StructField("pedido_id", IntegerType(), True),
    StructField("cliente_id", IntegerType(), True),
    StructField("valor", DoubleType(), True),
    StructField("data", StringType(), True)
])

# -------------------------
# 1️⃣ LEITURA DE DADOS
# -------------------------
try:
    df_clientes = spark.read.option("header", True).schema(schema_clientes).csv("data/input/clientes.csv")
    df_pedidos = spark.read.schema(schema_pedidos).json("data/input/pedidos.json")
    logging.info("Leitura de arquivos concluída com sucesso.")
except Exception as e:
    logging.error(f"Erro ao ler arquivos: {e}")
    raise

# -------------------------
# 2️⃣ VALIDAÇÃO DE DADOS COM GREAT EXPECTATIONS
# -------------------------
ge_df_clientes = ge.dataset.SparkDFDataset(df_clientes)
ge_df_pedidos = ge.dataset.SparkDFDataset(df_pedidos)

# Regras de validação
ge_df_clientes.expect_column_values_to_not_be_null("id")
ge_df_clientes.expect_column_values_to_be_of_type("idade", "IntegerType")

ge_df_pedidos.expect_column_values_to_not_be_null("pedido_id")
ge_df_pedidos.expect_column_values_to_not_be_null("valor")

clientes_result = ge_df_clientes.validate()
pedidos_result = ge_df_pedidos.validate()

# Separar registros válidos e rejeitados
df_clientes_validos = df_clientes
df_pedidos_validos = df_pedidos

os.makedirs("data/rejected", exist_ok=True)

if not clientes_result["success"]:
    logging.warning("Existem clientes rejeitados pelo Great Expectations!")
    df_clientes_validos = df_clientes.subtract(df_clientes)  # vazio
    df_clientes.write.mode("overwrite").csv("data/rejected/clientes_rejeitados.csv", header=True)
    pd.read_csv("data/rejected/clientes_rejeitados.csv").to_html("data/rejected/clientes_rejeitados.html")

if not pedidos_result["success"]:
    logging.warning("Existem pedidos rejeitados pelo Great Expectations!")
    df_pedidos_validos = df_pedidos.subtract(df_pedidos)  # vazio
    df_pedidos.write.mode("overwrite").csv("data/rejected/pedidos_rejeitados.csv", header=True)
    pd.read_csv("data/rejected/pedidos_rejeitados.csv").to_html("data/rejected/pedidos_rejeitados.html")

# -------------------------
# 3️⃣ ARMAZENAR DADOS NO DATA LAKE (S3)
# -------------------------
s3_path_clientes = "s3a://meu-data-lake/bronze/clientes/"
s3_path_pedidos = "s3a://meu-data-lake/bronze/pedidos/"

if df_clientes_validos.count() > 0:
    df_clientes_validos.write.mode("overwrite").parquet(s3_path_clientes)
    logging.info(f"Clientes válidos salvos no Data Lake: {s3_path_clientes}")

if df_pedidos_validos.count() > 0:
    df_pedidos_validos.write.mode("overwrite").parquet(s3_path_pedidos)
    logging.info(f"Pedidos válidos salvos no Data Lake: {s3_path_pedidos}")

# -------------------------
# 4️⃣ CARREGAR EM BANCO SQL
# -------------------------
jdbc_url = "jdbc:postgresql://localhost:5432/empresa"
db_properties = {
    "user": "postgres",
    "password": "senha123",
    "driver": "org.postgresql.Driver"
}

try:
    if df_clientes_validos.count() > 0:
        df_clientes_validos.write.mode("append").jdbc(jdbc_url, "clientes", properties=db_properties)
    if df_pedidos_validos.count() > 0:
        df_pedidos_validos.write.mode("append").jdbc(jdbc_url, "pedidos", properties=db_properties)
    logging.info("Dados carregados no banco SQL com sucesso.")
except Exception as e:
    logging.error(f"Erro ao carregar no banco SQL: {e}")

# -------------------------
# FINALIZAÇÃO
# -------------------------
logging.info("==== FIM DO PIPELINE ====")
spark.stop()
