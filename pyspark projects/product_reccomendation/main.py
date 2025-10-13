# ========================================
# Sistema de Recomendação Implícita com PySpark ALS
# Dataset Sintético com 200.000 interações
# ========================================

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import random

# 1️⃣ Criar SparkSession
spark = SparkSession.builder \
    .appName("RecomendacaoProdutosALSImpl") \
    .getOrCreate()

# 2️⃣ Gerar dataset sintético de interações (implicit feedback)
num_linhas = 200_000
num_usuarios = 5000
num_produtos = 1000

# rating = quantidade de compras (1 a 10)
dados_sinteticos = [(random.randint(1, num_usuarios),
                     random.randint(1, num_produtos),
                     random.randint(1, 10))  # número de interações
                    for _ in range(num_linhas)]

df = spark.createDataFrame(dados_sinteticos, ["userId", "productId", "rating"])

df.show(5)
print(f"Total de linhas: {df.count()}")

# 3️⃣ Dividir treino/teste
treino, teste = df.randomSplit([0.8, 0.2], seed=42)

# 4️⃣ Treinar modelo ALS implícito
als = ALS(
    userCol="userId",
    itemCol="productId",
    ratingCol="rating",
    rank=10,
    maxIter=15,
    regParam=0.1,
    implicitPrefs=True,  # ativa modo implícito
    alpha=1.0,           # confiança na interação
    coldStartStrategy="drop"
)

modelo = als.fit(treino)

# 5️⃣ Avaliar modelo (RMSE ainda pode ser usado, mas não é perfeito para implícito)
predicoes = modelo.transform(teste)
avaliador = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = avaliador.evaluate(predicoes)
print(f"RMSE do modelo implícito: {rmse:.4f}")

# 6️⃣ Recomendações para todos os usuários
recomendacoes_usuarios = modelo.recommendForAllUsers(5)
recomendacoes_usuarios.show(5, truncate=False)

# 7️⃣ Recomendações para todos os produtos
recomendacoes_produtos = modelo.recommendForAllItems(5)
recomendacoes_produtos.show(5, truncate=False)

# 8️⃣ Recomendações para usuário específico
usuario_id = 10
usuario_df = spark.createDataFrame([(usuario_id,)], ["userId"])
recomendacoes_usuario = modelo.recommendForUserSubset(usuario_df, 5)
recomendacoes_usuario.show(truncate=False)

# 9️⃣ (Opcional) Salvar modelo
# modelo.save("modelos/als_recomendacao_implicita")
