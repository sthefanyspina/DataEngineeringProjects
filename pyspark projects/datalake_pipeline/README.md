Data Lake Pipeline com PySpark – Bronze, Silver e Gold
📖 Descrição

Este projeto implementa um Data Lake em camadas utilizando PySpark. Ele simula um fluxo completo de ingestão, limpeza, deduplicação e enriquecimento de dados de vendas, seguindo a arquitetura Bronze → Silver → Gold:

Bronze: dados brutos (raw)

Silver: dados tratados e deduplicados

Gold: dados agregados e prontos para análise

O pipeline é totalmente automatizado e registra logs para rastreabilidade.

🗂 Estrutura do Projeto
data_lake_pipeline/
│
├── input/
│   └── vendas_200000.csv       # arquivo CSV com dados de vendas
│
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── logs/
│   └── pipeline_datalake_YYYYMMDD_HHMMSS.log
│
├── pipeline_datalake.py        # script PySpark do pipeline
└── README.md

⚙️ Pré-requisitos

Python 3.8+

Apache Spark 3.x

Pandas (para geração do dataset de teste)

Sistema operacional com capacidade para Spark (Windows, Linux, macOS)

📝 Instalação e Setup

Clone o repositório:

git clone https://github.com/seu-usuario/data_lake_pipeline.git
cd data_lake_pipeline


Instale dependências Python (opcional, apenas para geração do CSV de teste):

pip install pandas numpy


Crie a pasta input e gere o dataset de teste:

# Execute este script Python
import pandas as pd
import numpy as np

np.random.seed(42)
n = 200_000
id_venda = np.arange(1, n + 1)
nomes = ['Ana','Carlos','Beatriz','João','Marcos','Julia','Paula','Rafael','Fernanda','Lucas']
cliente = np.random.choice(nomes, size=n)
valor_venda = np.random.uniform(10,1000,size=n).round(2)
df = pd.DataFrame({'id_venda': id_venda, 'cliente': cliente, 'valor_venda': valor_venda})
df.to_csv("input/vendas_200000.csv", index=False)

🚀 Executando o Pipeline

Certifique-se de ter Spark instalado e configurado (spark-submit disponível).

Execute o pipeline:

spark-submit pipeline_datalake.py


Ao final da execução, os dados serão salvos nas camadas:

data_lake/bronze/vendas_raw.parquet

data_lake/silver/vendas_clean.parquet

data_lake/gold/vendas_aggregated.parquet

Logs de execução estarão em logs/.

🔧 Funcionalidades do Pipeline

Ingestão (Bronze): lê CSV bruto e salva em Parquet

Limpeza e deduplicação (Silver): padroniza texto, converte tipos e remove duplicados

Agregação e enriquecimento (Gold): gera métricas como total de vendas e quantidade de compras por cliente

Logs: registra todas as etapas com contagem de linhas e erros

📊 Exemplo de Resultado (Camada Gold)
cliente	total_vendas	qtd_compras
ana	200.0	2
carlos	150.0	1
beatriz	300.0	3