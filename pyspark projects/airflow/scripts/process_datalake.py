import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
import datetime
import os

spark = SparkSession.builder.appName("DataLakePipeline").getOrCreate()

# -------------------
# 1️⃣ Baixar dados da API (Bronze)
# -------------------
url = "https://api.covid19api.com/summary"
r = requests.get(url)
data = r.json()["Countries"]
df_bronze = pd.DataFrame(data)
bronze_path = "/opt/airflow/data/bronze/covid.csv"
df_bronze.to_csv(bronze_path, index=False)

# -------------------
# 2️⃣ Transformar (Silver)
# -------------------
df_spark = spark.read.option("header", True).csv(bronze_path)
df_silver = (
    df_spark
    .dropDuplicates(["Country"])
    .withColumn("Country", trim(lower(col("Country"))))
    .withColumn("Processed_Date", 
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
)
silver_path = "/opt/airflow/data/silver/covid.parquet"
df_silver.write.mode("overwrite").parquet(silver_path)

# -------------------
# 3️⃣ Agregação (Gold)
# -------------------
df_gold = df_silver.groupBy("Country").sum("TotalConfirmed")
gold_path = "/opt/airflow/data/gold/covid_summary.parquet"
df_gold.write.mode("overwrite").parquet(gold_path)

spark.stop()
