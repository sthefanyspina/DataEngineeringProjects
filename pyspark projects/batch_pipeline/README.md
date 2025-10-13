# 🟢 Pipeline de Ingestão Batch com PySpark

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.x-lightgrey)
![License](https://img.shields.io/badge/License-MIT-green)

## 🚀 Descrição

Pipeline de ingestão batch utilizando **PySpark**, com validação automática de dados (**Great Expectations**), armazenamento em **Data Lake (S3/GCS/HDFS)**, geração de **relatórios de erros** e carga em **banco SQL**. Ideal para ambientes de produção e testes com grandes volumes de dados.

---

## 📂 Estrutura do Projeto

pyspark_pipeline_batch/
│
├── dags/
│ └── ingestao_batch.py # DAG do Airflow
├── data/
│ ├── input/ # Arquivos CSV e JSON de entrada
│ └── rejected/ # Registros rejeitados
├── logs/
│ └── pipeline.log # Logs do pipeline
├── great_expectations/ # Validações automáticas
├── main_batch_pipeline.py # Script principal do pipeline
└── requirements.txt # Dependências do projeto

yaml
Copy code

---

## ⚙️ Pré-requisitos

- Python 3.8+
- Java 8+ (para Spark)
- Apache Spark 3.x
- Apache Airflow 2.x (opcional)
- Bibliotecas Python:

```bash
pip install pyspark pandas great_expectations psycopg2-binary
Banco de dados PostgreSQL ou outro compatível com JDBC

Acesso a Data Lake (S3 ou GCS)

🗄️ Configuração do Banco SQL
Exemplo PostgreSQL:

sql
Copy code
CREATE TABLE clientes (
    id INT PRIMARY KEY,
    nome VARCHAR(100),
    idade INT,
    renda DECIMAL
);

CREATE TABLE pedidos (
    pedido_id INT PRIMARY KEY,
    cliente_id INT,
    valor DECIMAL,
    data DATE
);
Configurar credenciais no main_batch_pipeline.py:

python
Copy code
jdbc_url = "jdbc:postgresql://localhost:5432/empresa"
db_properties = {
    "user": "postgres",
    "password": "senha123",
    "driver": "org.postgresql.Driver"
}
🏃 Execução do Pipeline
Manual
bash
Copy code
python main_batch_pipeline.py
Via Airflow
Copie a DAG dags/ingestao_batch.py para o diretório de DAGs do Airflow.

Inicie o scheduler e webserver:

bash
Copy code
airflow scheduler
airflow webserver
A DAG será executada diariamente conforme o agendamento configurado.

✅ Funcionalidades
Leitura de CSV e JSON da pasta data/input/.

Validação automática de dados usando Great Expectations:

Campos obrigatórios não nulos

Tipos de dados corretos

Registros rejeitados:

CSV: data/rejected/*.csv

HTML: data/rejected/*.html

Armazenamento no Data Lake:

Ex.: s3a://meu-data-lake/bronze/clientes/

Carga em banco SQL (PostgreSQL, MySQL ou SQL Server)

Logs detalhados em logs/pipeline.log.

📊 Dados de Teste
clientes.csv → +200.000 linhas

pedidos.json → +200.000 registros

Exemplo:

csv
Copy code
# clientes.csv
id,nome,idade,renda
1,Cliente_1,25,3500.50
2,Cliente_2,40,4200.00
json
Copy code
# pedidos.json
{"pedido_id":1,"cliente_id":1,"valor":150.75,"data":"2025-01-12"}
{"pedido_id":2,"cliente_id":2,"valor":320.40,"data":"2025-03-15"}