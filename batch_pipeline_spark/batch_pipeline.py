#Criar SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BatchIngestionPipeline") \
    .getOrCreate()

#Ler os dados (Extract)
raw_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/caminho/dados/vendas.csv")

#Transformar os dados (Transform)
from pyspark.sql.functions import col, to_date

clean_df = raw_df \
    .dropna(subset=["Order_id", "Price"]) \
    .withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd")) \
    .withColumn("Revenue", col("Price") * col("Quantity")) \
    .dropDuplicates(["Order_id"])

#Gravar os Dados (Load)
#Exemplo em Parquet
clean_df.write \
    .mode("overwrite") \
    .parquet("/caminho/output/vendas_parquet")

#Exemplo database
clean_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/meubanco") \
    .option("dbtable", "vendas_limpa") \
    .option("user", "usuario") \
    .option("password", "senha") \
    .mode("overwrite") \
    .save()

#Adicionar logs e metricas
import logging

logging.basicConfig(level=logging.INFO)
logging.info(f"Linhas lidas: {raw_df.count()}")
logging.info(f"Linhas finais: {clean_df.count()}")


