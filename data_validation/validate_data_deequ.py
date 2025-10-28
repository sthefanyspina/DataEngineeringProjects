# ==========================================
# validate_data_deequ.py
# ==========================================
import os
import json
from pyspark.sql import SparkSession
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *
from pydeequ.analyzers import *

# ------------------------------------------
# 1️⃣ Inicializa Spark com Deequ
# ------------------------------------------
spark = (
    SparkSession.builder
    .appName("DataQualityDeequ")
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3")
    .getOrCreate()
)

# ------------------------------------------
# 2️⃣ Carrega Dataset Sintético
# ------------------------------------------
data_path = "data/synthetic_data.csv"
if not os.path.exists(data_path):
    raise FileNotFoundError("❌ Dataset synthetic_data.csv não encontrado. Gere-o primeiro.")

df = spark.read.option("header", True).option("inferSchema", True).csv(data_path)
df.createOrReplaceTempView("synthetic_data")

print(f"✅ Dataset carregado: {df.count()} linhas, {len(df.columns)} colunas")

# ------------------------------------------
# 3️⃣ Define as Regras de Qualidade
# ------------------------------------------
check = (
    Check(spark, CheckLevel.Warning, "Data Quality Validation")
    # 1. Campos obrigatórios
    .isComplete("id")
    .isComplete("cpf")
    .isComplete("email")

    # 2. Unicidade de chave primária
    .isUnique("id")

    # 3. Valores dentro de faixa
    .isNonNegative("quantidade")
    .isContainedIn("status", ["ativo", "inativo", "pendente"])
    .isContainedIn("pais", ["BR", "US"])
    .isContainedIn("estado", ["SP", "RJ", "CA"])
    .isContainedIn("parcelas", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

    # 4. Formatos (regex)
    .hasPattern("cpf", r"^[0-9]{11}$", "CPF inválido")
    .hasPattern("email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "E-mail inválido")

    # 5. Campos numéricos
    .isGreaterThan("percentual", lowerBound=0.0)
    .isLessThan("percentual", upperBound=100.0)
    .isNonNegative("preco")

    # 6. Relações entre colunas
    .satisfies("data_inicio <= data_fim", "data_inicio_vs_data_fim")
    .satisfies("valor_total >= preco * quantidade - 10", "valor_total_minimo")
    .satisfies("valor_total <= preco * quantidade + 10", "valor_total_maximo")
    .satisfies("created_at <= updated_at", "created_before_updated")

    # 7. Campos de auditoria
    .isComplete("created_at")
    .hasCompleteness("updated_at", lambda x: x >= 0.98)

    # 8. Sem valores negativos indevidos
    .isNonNegative("valor_total")

    # 9. Outlier check (IQR simplificado)
    .satisfies("preco >= 0 and preco <= 2000", "preco_range")

    # 10. Volume e schema
    .hasSize(lambda s: s > 0)
    .hasColumn("id_cliente")
)

# ------------------------------------------
# 4️⃣ Executa as Validações
# ------------------------------------------
verifier = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check)

result = verifier.run()

# ------------------------------------------
# 5️⃣ Salva o Relatório JSON
# ------------------------------------------
os.makedirs("dq_reports", exist_ok=True)

result_json = VerificationResult.checkResultsAsJson(spark, result)
report_path = "dq_reports/deequ_report.json"

with open(report_path, "w", encoding="utf-8") as f:
    f.write(result_json)

print(f"📊 Relatório salvo em: {report_path}")

# ------------------------------------------
# 6️⃣ Exibe Resumo dos Resultados
# ------------------------------------------
result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
print("\nResumo das validações:")
result_df.show(truncate=False)

spark.stop()
