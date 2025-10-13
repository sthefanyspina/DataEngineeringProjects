# ========================================
# Visualização das Recomendações ALS Implícito
# ========================================

from pyspark.sql.functions import explode, col

# 1️⃣ Recomendações para todos os usuários (top 5)
recomendacoes_usuarios = modelo.recommendForAllUsers(5)

# 2️⃣ Explodir a coluna de recomendações em linhas separadas
recomendacoes_explodidas = recomendacoes_usuarios.select(
    col("userId"),
    explode(col("recommendations")).alias("rec")
)

# 3️⃣ Separar produto e score
recomendacoes_tabela = recomendacoes_explodidas.select(
    col("userId"),
    col("rec.productId").alias("produto"),
    col("rec.rating").alias("score")
)

# 4️⃣ Mostrar os top 20 registros
recomendacoes_tabela.orderBy("userId").show(20)
