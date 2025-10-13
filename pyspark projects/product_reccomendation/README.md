Sistema de Recomendação de Produtos com PySpark ALS






📌 Descrição

Sistema de recomendação de produtos usando PySpark MLlib e ALS (Alternating Least Squares).
Suporta:

Ratings explícitos (1–5)

Interações implícitas (quantidade de compras ou cliques)

Gera recomendações para usuários e produtos, com visualização tabular (usuário → produtos + score).

⚡ Funcionalidades

Dataset sintético com 200.000 linhas

Treinamento ALS explícito e implícito

Avaliação do modelo (RMSE)

Recomendações para todos os usuários, produtos ou subset de usuários

Exportação de recomendações em CSV/Parquet

🚀 Como Usar
Instalar dependências
pip install pyspark

Executar script
spark-submit scripts/als_implicito.py

Exibir recomendações de um usuário específico
usuario_id = 10
usuario_df = spark.createDataFrame([(usuario_id,)], ["userId"])
recomendacoes_usuario = modelo.recommendForUserSubset(usuario_df, 5)
recomendacoes_usuario.show(truncate=False)

Salvar recomendações
recomendacoes_tabela.write.csv("recomendacoes_usuarios.csv", header=True)

📂 Estrutura do Projeto
recomendacao-produtos/
│
├─ data/                  
├─ notebooks/             
├─ modelos/               
├─ scripts/               
│   ├─ als_explicito.py   
│   └─ als_implicito.py   
├─ README.md
└─ requirements.txt

📊 Dataset

userId: ID do usuário

productId: ID do produto

rating: nota explícita (1–5) ou interações (1–10)

Pode-se usar datasets reais, como MovieLens
 ou logs de e-commerce.

🔹 Observações

ALS implícito é recomendado para dados de interações sem nota explícita

alpha define a confiança nas interações múltiplas

Para grandes datasets, use cluster Spark ou Databricks