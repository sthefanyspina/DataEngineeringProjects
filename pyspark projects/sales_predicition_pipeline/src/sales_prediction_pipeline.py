# ========================================
# Projeto PySpark - Pipeline de Previsão de Vendas
# ========================================

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, dayofweek, when, avg
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# -------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Iniciando pipeline de previsão de vendas")

# -------------------------
# 1. CRIAR SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("Pipeline_Previsao_Vendas") \
    .getOrCreate()

# -------------------------
# 2. CARREGAR DADOS
# -------------------------
df = spark.read.csv("data/vendas.csv", header=True, inferSchema=True)
logging.info(f"Linhas carregadas: {df.count()}")

# -------------------------
# 3. FEATURE ENGINEERING
# -------------------------
df = df.withColumn("mes", month(col("data"))) \
       .withColumn("dia_semana", dayofweek(col("data"))) \
       .withColumn("fim_semana", when(col("dia_semana").isin(1,7), 1).otherwise(0))

# Média móvel 7 dias
janela = Window.partitionBy("loja", "produto").orderBy("data").rowsBetween(-7, -1)
df = df.withColumn("media_movel_7d", avg("vendas").over(janela))

# Flag de feriado
df = df.withColumn("feriado", when(col("data").isin(["2023-01-01", "2023-12-25"]), 1).otherwise(0))

# -------------------------
# 4. MONTAGEM DO PIPELINE
# -------------------------
feature_cols = ["mes", "dia_semana", "fim_semana", "media_movel_7d", "feriado"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
lr = LinearRegression(featuresCol="features", labelCol="vendas")
pipeline = Pipeline(stages=[assembler, lr])

# -------------------------
# 5. TREINAR E AVALIAR
# -------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
modelo = pipeline.fit(train_df)
predicoes = modelo.transform(test_df)

avaliador = RegressionEvaluator(labelCol="vendas", predictionCol="prediction", metricName="rmse")
rmse = avaliador.evaluate(predicoes)
r2 = avaliador.evaluate(predicoes, {avaliador.metricName: "r2"})

logging.info(f"Avaliação do modelo -> RMSE: {rmse:.2f}, R²: {r2:.2f}")
print(f"RMSE: {rmse:.2f}")
print(f"R²: {r2:.2f}")

# -------------------------
# 6. SALVAR MODELO E PREVISÕES
# -------------------------
os.makedirs("modelos", exist_ok=True)
modelo.write().overwrite().save("modelos/previsao_vendas_lr")

os.makedirs("output", exist_ok=True)
predicoes.select("data", "loja", "produto", "vendas", "prediction") \
    .write.mode("overwrite").parquet("output/previsoes.parquet")

logging.info("Pipeline finalizado com sucesso.")
print("✅ Pipeline executado com sucesso!")
