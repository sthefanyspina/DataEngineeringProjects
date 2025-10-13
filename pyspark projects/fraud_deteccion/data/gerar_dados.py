from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
import random
import datetime

# -------------------------
# Criar sessão Spark
# -------------------------
spark = SparkSession.builder \
    .appName("GenerateFraudDataset") \
    .getOrCreate()

# -------------------------
# Configuração do dataset
# -------------------------
num_rows = 200000
merchant_types = ["ecommerce", "grocery", "luxury_goods", "electronics", "travel", "fashion"]
fraud_probability = 0.02  # 2% das transações são fraudes

# -------------------------
# Gerar dados sintéticos
# -------------------------
data = []
start_date = datetime.datetime(2025, 1, 1)

for i in range(1, num_rows + 1):
    amount = round(random.uniform(5, 5000), 2)
    time = start_date + datetime.timedelta(minutes=random.randint(0, 365*24*60))
    merchant = random.choice(merchant_types)
    is_fraud = 1 if random.random() < fraud_probability else 0
    data.append((i, amount, time.strftime("%Y-%m-%d %H:%M:%S"), merchant, is_fraud))

# -------------------------
# Criar DataFrame Spark
# -------------------------
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("time", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("is_fraud", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
df.show(5)

# -------------------------
# Salvar CSV para o pipeline
# -------------------------
df.write.csv("data/transactions.csv", header=True, mode="overwrite")
print("Dataset de transações criado em 'data/transactions.csv' com 200.000 linhas.")

# -------------------------
# Finalizar Spark
# -------------------------
spark.stop()
