# ========================================
# Projeto PySpark - Análise Avançada de Logs Web
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, hour, count, countDistinct, when, round
)
import logging
import os

# -------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/analise_logs_web_avancada.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Iniciando análise avançada de logs web...")

try:
    # -------------------------
    # INICIAR SPARK SESSION
    # -------------------------
    spark = SparkSession.builder \
        .appName("AnaliseAvancadaLogsWeb") \
        .getOrCreate()
    logging.info("SparkSession criada com sucesso.")

    # -------------------------
    # LER ARQUIVOS DE LOG
    # -------------------------
    log_path = "/data/logs/*.log"  # ajuste o caminho conforme seu ambiente
    logs_df = spark.read.text(log_path)
    logging.info(f"Arquivos de log lidos de {log_path}.")

    # -------------------------
    # PADRÕES REGEX PARA APACHE/Nginx
    # Exemplo de linha:
    # 192.168.1.10 - - [12/Oct/2025:10:45:22 -0300] "GET /produtos HTTP/1.1" 200 5321
    # -------------------------
    ip_pattern = r'(^\S+)'  # IP
    datetime_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})'  # Data e hora
    method_pattern = r'\"(GET|POST|PUT|DELETE)'  # Método HTTP
    url_pattern = r'\"(?:GET|POST|PUT|DELETE) (\S+)'  # Caminho
    status_pattern = r'\" (\d{3})'  # Código de status
    size_pattern = r' (\d+)$'  # Tamanho da resposta

    logs_limpos = logs_df.select(
        regexp_extract(col("value"), ip_pattern, 1).alias("ip"),
        regexp_extract(col("value"), datetime_pattern, 1).alias("datetime"),
        regexp_extract(col("value"), method_pattern, 1).alias("metodo"),
        regexp_extract(col("value"), url_pattern, 1).alias("url"),
        regexp_extract(col("value"), status_pattern, 1).alias("status"),
        regexp_extract(col("value"), size_pattern, 1).alias("tamanho")
    )

    # -------------------------
    # CONVERTER CAMPOS E ADICIONAR COLUNAS DERIVADAS
    # -------------------------
    logs_limpos = logs_limpos.withColumn(
        "timestamp", to_timestamp(col("datetime"), "dd/MMM/yyyy:HH:mm:ss")
    ).withColumn(
        "hora", hour(col("timestamp"))
    ).withColumn(
        "status_int", col("status").cast("int")
    ).withColumn(
        "erro", when(col("status_int") >= 400, 1).otherwise(0)
    )

    logging.info("Campos extraídos e enriquecidos com sucesso.")

    # -------------------------
    # MÉTRICAS 1: ACESSOS POR HORA
    # -------------------------
    acessos_por_hora = logs_limpos.groupBy("hora").count().orderBy("hora")

    # -------------------------
    # MÉTRICAS 2: PÁGINAS MAIS ACESSADAS
    # -------------------------
    paginas_populares = logs_limpos.groupBy("url").count().orderBy(col("count").desc())

    # -------------------------
    # MÉTRICAS 3: IPs MAIS ATIVOS
    # -------------------------
    ips_ativos = logs_limpos.groupBy("ip").count().orderBy(col("count").desc())

    # -------------------------
    # MÉTRICAS 4: ERROS HTTP
    # -------------------------
    erros_http = logs_limpos.filter(col("status_int") >= 400) \
        .groupBy("status").count().orderBy(col("count").desc())

    # -------------------------
    # MÉTRICAS 5: TAXA DE ERRO (%)
    # -------------------------
    total = logs_limpos.count()
    total_erros = logs_limpos.filter(col("erro") == 1).count()
    taxa_erro = round((total_erros / total) * 100, 2) if total > 0 else 0

    logging.info(f"Total de requisições: {total}")
    logging.info(f"Total de erros HTTP: {total_erros}")
    logging.info(f"Taxa de erro: {taxa_erro}%")

    # -------------------------
    # EXIBIR RESULTADOS NO CONSOLE
    # -------------------------
    print("\n=== Acessos por Hora ===")
    acessos_por_hora.show(24, truncate=False)

    print("\n=== Páginas Mais Acessadas ===")
    paginas_populares.show(10, truncate=False)

    print("\n=== IPs Mais Ativos ===")
    ips_ativos.show(10, truncate=False)

    print("\n=== Erros HTTP ===")
    erros_http.show(truncate=False)

    print(f"\nTaxa de erro geral: {taxa_erro}%")

    # -------------------------
    # SALVAR RESULTADOS EM PARQUET
    # -------------------------
    os.makedirs("/data/output/", exist_ok=True)
    acessos_por_hora.write.mode("overwrite").parquet("/data/output/acessos_por_hora/")
    paginas_populares.write.mode("overwrite").parquet("/data/output/paginas_populares/")
    ips_ativos.write.mode("overwrite").parquet("/data/output/ips_ativos/")
    erros_http.write.mode("overwrite").parquet("/data/output/erros_http/")

    logging.info("Resultados salvos em /data/output/ com sucesso.")

except Exception as e:
    logging.error(f"Erro durante a execução: {e}")
    raise e

finally:
    spark.stop()
    logging.info("SparkSession encerrada.")
