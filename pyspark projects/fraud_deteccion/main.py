# ========================================
# Projeto PySpark Profissional - Detecção de Fraudes
# ========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, when
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os
import logging

# -------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/fraud_detection_pro.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Iniciando pipeline profissional de detecção de fraudes.")

# -------------------------
# CRIAR SESSÃO SPARK
# -------------------------
spark = SparkSession.builder \
    .appName("DeteccaoFraudesPro") \
    .getOrCreate()
logging.info("Sessão Spark criada.")

# -------------------------
# CARREGAR DADOS
# -------------------------
df = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)
logging.info(f"Dados carregados. Total de registros: {df.count()}")

# -------------------------
# TRATAMENTO DE DADOS
# -------------------------
df = df.dropna(subset=["amount", "merchant_type", "time", "is_fraud"])
df = df.withColumn("amount", when(col("amount") < 0, 0).otherwise(col("amount")))

# Features temporais
df = df.withColumn("hour", hour(col("time").cast("timestamp"))) \
       .withColumn("day_of_week", dayofweek(col("time").cast("timestamp"))) \
       .withColumn("month", month(col("time").cast("timestamp")))

# -------------------------
# BALANCEAMENTO DE CLASSES
# -------------------------
# Para oversampling simples: replicar fraudes (SMOTE não nativo em PySpark)
fraud_df = df.filter(col("is_fraud") == 1)
nonfraud_df = df.filter(col("is_fraud") == 0)

fraud_count = fraud_df.count()
nonfraud_count = nonfraud_df.count()
replication_factor = int(nonfraud_count / fraud_count) - 1

# Replicar fraudes para balancear
oversampled_fraud = fraud_df
for _ in range(replication_factor):
    oversampled_fraud = oversampled_fraud.union(fraud_df)

df_balanced = nonfraud_df.union(oversampled_fraud)
logging.info(f"Total após oversampling: {df_balanced.count()} (fraudes replicadas {replication_factor}x)")

# -------------------------
# ENGENHARIA DE FEATURES
# -------------------------
indexer = StringIndexer(inputCol="merchant_type", outputCol="merchant_index")
encoder = OneHotEncoder(inputCols=["merchant_index"], outputCols=["merchant_vec"])

assembler = VectorAssembler(
    inputCols=["amount", "hour", "day_of_week", "month", "merchant_vec"],
    outputCol="features_raw"
)

scaler = StandardScaler(inputCol="features_raw", outputCol="features")

# -------------------------
# MODELOS
# -------------------------
models = {
    "LogisticRegression": LogisticRegression(labelCol="is_fraud", featuresCol="features"),
    "DecisionTree": DecisionTreeClassifier(labelCol="is_fraud", featuresCol="features"),
    "RandomForest": RandomForestClassifier(labelCol="is_fraud", featuresCol="features", numTrees=100),
    "GBT": GBTClassifier(labelCol="is_fraud", featuresCol="features", maxIter=50)
}

# -------------------------
# DIVIDIR TREINO / TESTE
# -------------------------
train, test = df_balanced.randomSplit([0.8, 0.2], seed=42)
logging.info(f"Conjunto de treino: {train.count()} | Conjunto de teste: {test.count()}")

# -------------------------
# AVALIADOR
# -------------------------
evaluator = BinaryClassificationEvaluator(
    labelCol="is_fraud",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

# -------------------------
# TREINAR E AVALIAR MODELOS COM CROSS-VALIDATION
# -------------------------
results = {}
best_models = {}

for name, classifier in models.items():
    logging.info(f"Iniciando treinamento: {name}")
    
    pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, classifier])
    
    # Criar grid de hiperparâmetros simples
    if name == "RandomForest":
        paramGrid = ParamGridBuilder() \
            .addGrid(classifier.numTrees, [50, 100]) \
            .addGrid(classifier.maxDepth, [5, 10]) \
            .build()
    elif name == "GBT":
        paramGrid = ParamGridBuilder() \
            .addGrid(classifier.maxIter, [50, 100]) \
            .addGrid(classifier.maxDepth, [3, 5]) \
            .build()
    else:
        paramGrid = ParamGridBuilder().build()  # LR e DT sem grid simples
    
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2
    )
    
    cv_model = cv.fit(train)
    predictions = cv_model.transform(test)
    roc_auc = evaluator.evaluate(predictions)
    
    results[name] = roc_auc
    best_models[name] = cv_model
    logging.info(f"AUC ROC {name}: {roc_auc}")
    print(f"AUC ROC {name}: {roc_auc}")

# -------------------------
# SALVAR MELHOR MODELO
# -------------------------
best_model_name = max(results, key=results.get)
logging.info(f"Melhor modelo final: {best_model_name} com AUC {results[best_model_name]}")

best_model = best_models[best_model_name].bestModel
best_model.save(f"models/fraud_detection_{best_model_name}_pro")
logging.info(f"Modelo salvo em 'models/fraud_detection_{best_model_name}_pro'.")

# -------------------------
# FINALIZAR SPARK
# -------------------------
spark.stop()
logging.info("Pipeline profissional finalizado com sucesso.")
