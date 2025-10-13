# ========================================
# Projeto PySpark - Contagem de Palavras (DataFrame)
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace
import os

# -------------------------
# CONFIGURAÇÃO DE PASTAS
# -------------------------
os.makedirs("saida_df", exist_ok=True)

# -------------------------
# CRIAR SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("WordCountDF") \
    .getOrCreate()

# -------------------------
# LER ARQUIVO DE TEXTO
# -------------------------
arquivo = "texto.txt"  # Coloque seu arquivo aqui
df = spark.read.text(arquivo)

# -------------------------
# LIMPAR E SEPARAR PALAVRAS
# -------------------------
# - Remover pontuação
# - Transformar em minúsculas
# - Separar palavras em linhas individuais
df_palavras = df.select(
    explode(
        split(
            regexp_replace(lower(col("value")), r'[^a-z0-9\s]', ''), 
            r'\s+'
        )
    ).alias("palavra")
).filter(col("palavra") != "")  # remover strings vazias

# -------------------------
# CONTAR FREQUÊNCIA DE CADA PALAVRA
# -------------------------
df_contagem = df_palavras.groupBy("palavra").count().orderBy(col("count").desc())

# -------------------------
# SALVAR RESULTADO EM ARQUIVO
# -------------------------
df_contagem.write.mode("overwrite").csv("saida_df/contagem_palavras", header=True)

# -------------------------
# OPCIONAL: MOSTRAR NO TERMINAL
# -------------------------
df_contagem.show(truncate=False)

# -------------------------
# FECHAR SPARK SESSION
# -------------------------
spark.stop()
