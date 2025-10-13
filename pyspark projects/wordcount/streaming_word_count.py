# ========================================
# Projeto PySpark - Contagem de Palavras (Streaming)
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, regexp_replace
import os

# -------------------------
# CONFIGURAÇÃO DE PASTAS
# -------------------------
input_dir = "stream_input"   # pasta onde os arquivos serão adicionados
output_dir = "stream_output" # pasta para salvar resultado
os.makedirs(input_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# -------------------------
# CRIAR SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("WordCountStreaming") \
    .getOrCreate()

# -------------------------
# LER ARQUIVOS EM STREAMING
# -------------------------
df = spark.readStream.text(input_dir)

# -------------------------
# LIMPAR E SEPARAR PALAVRAS
# -------------------------
df_palavras = df.select(
    explode(
        split(
            regexp_replace(lower(col("value")), r'[^a-z0-9\s]', ''), 
            r'\s+'
        )
    ).alias("palavra")
).filter(col("palavra") != "")

# -------------------------
# CONTAR FREQUÊNCIA DE CADA PALAVRA
# -------------------------
df_contagem = df_palavras.groupBy("palavra").count()

# -------------------------
# DEFINIR OUTPUT DO STREAMING
# -------------------------
query = df_contagem.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# -------------------------
# MANTER O STREAM ATIVO
# -------------------------
query.awaitTermination()
