from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, to_date, lit, udf, array, concat_ws
from pyspark.sql.types import BooleanType, StringType
import logging
import os

# -------------------------
# Configuração de log
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline_normalizacao_avancado.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Iniciando pipeline avançado de normalização")

# -------------------------
# Criar Spark Session
# -------------------------
spark = SparkSession.builder.appName("PipelineNormalizacaoAvancado").getOrCreate()

# -------------------------
# Leitura de dados brutos
# -------------------------
df = spark.read.csv("data/raw/clientes.csv", header=True, sep=",", inferSchema=True)
logging.info(f"Linhas lidas: {df.count()}")

# -------------------------
# Normalização inicial
# -------------------------
df_norm = (
    df
    .withColumn("nome", upper(trim(col("nome"))))
    .withColumn("cpf", regexp_replace(col("cpf"), r"[^0-9]", ""))
    .withColumn("telefone", regexp_replace(col("telefone"), r"[^0-9]", ""))
    .withColumn("data_nascimento", to_date(col("data_nascimento"), "dd/MM/yyyy"))
)

# -------------------------
# Função para validar CPF
# -------------------------
def validar_cpf(cpf):
    if not cpf or len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    # Cálculo do primeiro dígito
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    d1 = (soma * 10 % 11) % 10
    # Cálculo do segundo dígito
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    d2 = (soma * 10 % 11) % 10
    return d1 == int(cpf[9]) and d2 == int(cpf[10])

validar_cpf_udf = udf(validar_cpf, BooleanType())

# -------------------------
# Função para validar telefone
# -------------------------
def validar_telefone(telefone):
    return telefone is not None and (10 <= len(telefone) <= 11)

validar_telefone_udf = udf(validar_telefone, BooleanType())

# -------------------------
# Criar colunas de validação
# -------------------------
df_valid = df_norm \
    .withColumn("cpf_valido", validar_cpf_udf(col("cpf"))) \
    .withColumn("telefone_valido", validar_telefone_udf(col("telefone"))) \
    .withColumn("data_valida", col("data_nascimento").isNotNull()) \
    .withColumn("nome_valido", (col("nome") != ""))

# -------------------------
# Gerar coluna com todos os motivos de rejeição
# -------------------------
def motivos_rejeicao(cpf_valido, telefone_valido, data_valida, nome_valido):
    motivos = []
    if not cpf_valido:
        motivos.append("CPF inválido")
    if not telefone_valido:
        motivos.append("Telefone inválido")
    if not data_valida:
        motivos.append("Data inválida")
    if not nome_valido:
        motivos.append("Nome vazio")
    return ",".join(motivos) if motivos else None

motivos_rejeicao_udf = udf(motivos_rejeicao, StringType())

df_final = df_valid.withColumn(
    "motivo_rejeicao",
    motivos_rejeicao_udf(
        col("cpf_valido"),
        col("telefone_valido"),
        col("data_valida"),
        col("nome_valido")
    )
)

# -------------------------
# Separar dados válidos e rejeitados
# -------------------------
df_limpo = df_final.filter(col("motivo_rejeicao").isNull()) \
    .drop("cpf_valido", "telefone_valido", "data_valida", "nome_valido", "motivo_rejeicao")

df_rejeitados = df_final.filter(col("motivo_rejeicao").isNotNull()) \
    .drop("cpf_valido", "telefone_valido", "data_valida", "nome_valido")

logging.info(f"Registros válidos: {df_limpo.count()}")
logging.info(f"Registros rejeitados: {df_rejeitados.count()}")

# -------------------------
# Salvar resultados
# -------------------------
os.makedirs("data/processed", exist_ok=True)

df_limpo.write.mode("overwrite").parquet("data/processed/dados_limpios.parquet")
df_rejeitados.write.mode("overwrite").csv("data/processed/dados_rejeitados.csv", header=True)

logging.info("Pipeline avançado concluído. Dados limpos e rejeitados salvos.")

spark.stop()
