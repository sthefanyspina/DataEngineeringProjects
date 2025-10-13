🏭 Airflow + PySpark Data Lake Pipeline










Pipeline diário de dados que utiliza Apache Airflow e PySpark, seguindo a arquitetura Bronze → Silver → Gold, baixando dados de uma API pública (COVID-19), transformando e salvando em um Data Lake local.

📂 Estrutura do Projeto
airflow_spark/
│
├── dags/
│   └── pyspark_datalake_dag.py       # DAG do Airflow
│
├── scripts/
│   └── process_datalake.py           # Script PySpark
│
├── data/
│   ├── bronze/                       # Dados brutos (CSV)
│   ├── silver/                       # Dados tratados (Parquet)
│   └── gold/                         # Dados agregados (Parquet)
│
├── docker-compose.yml                 # Containers Airflow + Spark
└── requirements.txt                   # Dependências Python

⚙️ Pré-requisitos

Docker & Docker Compose

Internet para baixar dados da API

Opcional: Para salvar no S3 ou GCS, configure as credenciais correspondentes e altere os caminhos no script PySpark.

🚀 Como rodar o projeto

Clone o repositório:

git clone https://github.com/seu-usuario/airflow_spark.git
cd airflow_spark


Suba os containers:

docker-compose up -d


Acesse o Airflow Web UI:

http://localhost:8080


Ative a DAG pyspark_datalake_dag.
Você pode executar manualmente clicando em Trigger DAG ou aguardar a execução diária automática.

🧩 Fluxo do Pipeline
Etapa	Descrição	Camada
Ingestão	Baixa dados da API COVID-19	Bronze
Transformação	Limpeza e normalização	Silver
Agregação	KPIs e resumos por país	Gold
Agendamento	Rodado diariamente pelo Airflow	Automático
📌 Tecnologias

Apache Airflow: Orquestração de workflows

PySpark: Processamento distribuído de dados

Docker: Contêinerização para execução isolada

Pandas: Leitura inicial da API

🗄️ Data Lake Local

data/bronze/ → CSV bruto

data/silver/ → Parquet tratado

data/gold/ → Parquet agregado

Para nuvem, altere os caminhos no script PySpark para S3 ou GCS.

🔧 Customizações

Alterar a API de origem no script process_datalake.py

Alterar a frequência de execução no DAG (schedule_interval)

Adicionar novas transformações ou agregações no script PySpark

💡 Observações

Pipeline extensível para qualquer fonte de dados e transformações

Adaptável para produção, salvando dados diretamente em AWS S3, GCP Storage ou HDFS

📈 Visualização do Pipeline
[API COVID-19] → [Bronze CSV] → [Silver Parquet] → [Gold Parquet]
        │                 │                  │
        └─────────────── DAG Airflow ────────┘