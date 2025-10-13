# ========================================
# Projeto PySpark - Leitura, Transformação e EDA de CSV
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, to_date, avg, count, countDistinct,
    regexp_replace
)
import matplotlib.pyplot as plt
import pandas as pd
import time
import logging
import os

# -------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------
# INICIAR SPARK
# -------------------------
spark = SparkSession.builder \
    .appName("Projeto_PySpark_CSV") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

logging.info("SparkSession iniciada com sucesso.")

# -------------------------
# 1. LEITURA DE CSVs GRANDES
# -------------------------
logging.info("Lendo arquivos CSV...")

df_clientes = spark.read.csv("data/clientes.csv", header=True, inferSchema=True)
df_produtos = spark.read.csv("data/produtos.csv", header=True, inferSchema=True)
df_vendas = spark.read.csv("data/vendas.csv", header=True, inferSchema=True)

print("✅ Arquivos CSV carregados com sucesso.")
print("Clientes:", df_clientes.count(), "| Produtos:", df_produtos.count(), "| Vendas:", df_vendas.count())

# -------------------------
# 2. LIMPEZA E NORMALIZAÇÃO
# -------------------------
logging.info("Limpando e normalizando colunas...")

# Normalizar nomes das colunas
for df_name, df in [("clientes", df_clientes), ("produtos", df_produtos), ("vendas", df_vendas)]:
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    if df_name == "clientes":
        df_clientes = df
    elif df_name == "produtos":
        df_produtos = df
    else:
        df_vendas = df

# Corrigir e padronizar dados
df_clientes = df_clientes.withColumn("nome", trim(lower(col("nome")))) \
    .withColumn("cidade", trim(lower(col("cidade")))) \
    .withColumn("estado", trim(upper(col("estado")))) \
    .dropna(subset=["id_cliente", "nome"])

df_produtos = df_produtos.withColumn("produto", trim(lower(col("produto")))) \
    .withColumn("categoria", trim(lower(col("categoria")))) \
    .withColumn("preco", regexp_replace(col("preco"), ",", ".").cast("double")) \
    .dropna(subset=["id_produto", "produto", "preco"])

df_vendas = df_vendas.withColumn("produto", trim(lower(col("produto")))) \
    .withColumn("data_venda", to_date(col("data_venda"), "yyyy-MM-dd")) \
    .withColumn("valor_venda", col("valor_venda").cast("double")) \
    .dropna(subset=["id_venda", "id_cliente", "id_produto", "valor_venda"]) \
    .dropDuplicates(["id_venda"])

# -------------------------
# 3. SALVAR EM PARQUET
# -------------------------
os.makedirs("output/parquet", exist_ok=True)
df_vendas.write.mode("overwrite").parquet("output/parquet/vendas.parquet")
logging.info("Arquivo Parquet salvo com sucesso.")

# -------------------------
# 4. ANÁLISE EXPLORATÓRIA (EDA)
# -------------------------
logging.info("Iniciando análise exploratória...")

print("\n📊 Resumo estatístico das vendas:")
df_vendas.describe(["valor_venda"]).show()

print("\n📈 Média e quantidade de vendas por produto:")
df_agg = df_vendas.groupBy("produto") \
    .agg(
        count("*").alias("qtd_vendas"),
        avg("valor_venda").alias("media_valor")
    ) \
    .orderBy("qtd_vendas", ascending=False)

df_agg.show(10)

# Converter para Pandas para visualizações
df_agg_pd = df_agg.limit(10).toPandas()

# --- Gráfico 1: Top 10 produtos mais vendidos ---
plt.figure(figsize=(10, 6))
plt.barh(df_agg_pd["produto"], df_agg_pd["qtd_vendas"], color="skyblue")
plt.xlabel("Quantidade de Vendas")
plt.ylabel("Produto")
plt.title("Top 10 Produtos Mais Vendidos")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("output/top10_produtos.png")
plt.show()

# --- Gráfico 2: Distribuição de valores de venda ---
df_vendas_pd = df_vendas.sample(0.05).toPandas()  # 5% da amostra
plt.figure(figsize=(8, 5))
plt.hist(df_vendas_pd["valor_venda"], bins=30, color="lightcoral", edgecolor="black")
plt.title("Distribuição de Valores de Venda")
plt.xlabel("Valor da Venda (R$)")
plt.ylabel("Frequência")
plt.tight_layout()
plt.savefig("output/distribuicao_valor_venda.png")
plt.show()

# --- Gráfico 3: Evolução das vendas ao longo do tempo ---
df_tempo = df_vendas.groupBy("data_venda").agg(count("*").alias("qtd_vendas")).orderBy("data_venda")
df_tempo_pd = df_tempo.toPandas()
plt.figure(figsize=(10, 5))
plt.plot(df_tempo_pd["data_venda"], df_tempo_pd["qtd_vendas"], color="green")
plt.title("Evolução de Vendas ao Longo do Tempo")
plt.xlabel("Data")
plt.ylabel("Quantidade de Vendas")
plt.tight_layout()
plt.savefig("output/evolucao_vendas.png")
plt.show()

# -------------------------
# 5. CONVERSÃO DE FORMATOS
# -------------------------
logging.info("Convertendo formatos CSV → Parquet → JSON...")

df_parquet = spark.read.parquet("output/parquet/vendas.parquet")
df_parquet.write.mode("overwrite").json("output/json/vendas.json")

# -------------------------
# 6. MEDIÇÃO DE DESEMPENHO
# -------------------------
logging.info("Medindo desempenho de leitura CSV vs Parquet...")

start = time.time()
spark.read.csv("data/vendas.csv", header=True, inferSchema=True).count()
csv_time = time.time() - start

start = time.time()
spark.read.parquet("output/parquet/vendas.parquet").count()
parquet_time = time.time() - start

print(f"\n⚙️ Tempo de leitura CSV: {csv_time:.2f}s | Parquet: {parquet_time:.2f}s")
logging.info(f"Tempo CSV: {csv_time:.2f}s | Parquet: {parquet_time:.2f}s")

# -------------------------
# 7. JUNÇÕES ENTRE DATASETS
# -------------------------
logging.info("Realizando junções entre clientes, produtos e vendas...")

df_vendas_clientes = df_vendas.join(df_clientes, on="id_cliente", how="left")
df_final = df_vendas_clientes.join(df_produtos, on="id_produto", how="left")

# Tratar duplicatas e valores nulos
df_final = df_final.dropDuplicates(["id_venda"])
df_final = df_final.fillna({
    "categoria": "desconhecida",
    "cidade": "não informado",
    "estado": "XX"
})

print("\n🔗 Dataset final após join:")
df_final.show(5)

# -------------------------
# 8. SALVAR RESULTADO FINAL
# -------------------------
df_final.write.mode("overwrite").parquet("output/parquet/dataset_final.parquet")
logging.info("Dataset final salvo em Parquet com sucesso.")

# -------------------------
# 9. ENCERRAR SPARK
# -------------------------
spark.stop()
logging.info("Pipeline finalizado com sucesso.")
print("\n✅ Pipeline concluído com sucesso!")
