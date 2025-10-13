# ========================================
# Projeto PySpark - Contagem de Palavras
# ========================================

from pyspark.sql import SparkSession
import re
import os

# -------------------------
# CONFIGURAÇÃO DE PASTAS
# -------------------------
os.makedirs("saida", exist_ok=True)

# -------------------------
# CRIAR SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# -------------------------
# LER ARQUIVO DE TEXTO
# -------------------------
arquivo = "texto.txt"  # coloque seu arquivo de texto aqui
rdd = spark.sparkContext.textFile(arquivo)

# -------------------------
# LIMPAR E SEPARAR PALAVRAS
# -------------------------
# - Transformar em minúsculas
# - Remover pontuação
# - Separar palavras
palavras = rdd.flatMap(lambda linha: re.findall(r'\b\w+\b', linha.lower()))

# -------------------------
# CRIAR PARES (palavra, 1)
# -------------------------
pares = palavras.map(lambda palavra: (palavra, 1))

# -------------------------
# SOMAR OCORRÊNCIAS
# -------------------------
contagem = pares.reduceByKey(lambda a, b: a + b)

# -------------------------
# SALVAR RESULTADO EM ARQUIVO
# -------------------------
contagem.saveAsTextFile("saida/contagem_palavras")

# -------------------------
# OPCIONAL: IMPRIMIR NO TERMINAL
# -------------------------
for palavra, qtd in contagem.collect():
    print(f"{palavra}: {qtd}")

# -------------------------
# FECHAR SPARK SESSION
# -------------------------
spark.stop()
