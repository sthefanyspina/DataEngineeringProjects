from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import sqlite3

# ---------- CONFIGURAÇÕES ----------
URL = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"  # CSV público de exemplo
DB_PATH = "/tmp/meu_banco.db"
TABLE_NAME = "dados_processados"

# ---------- FUNÇÕES DAS TAREFAS ----------
def baixar_csv(**context):
    response = requests.get(URL)
    csv_path = "/tmp/dados.csv"
    with open(csv_path, "wb") as f:
        f.write(response.content)
    context['ti'].xcom_push(key='csv_path', value=csv_path)

def transformar_dados(**context):
    csv_path = context['ti'].xcom_pull(key='csv_path')
    df = pd.read_csv(csv_path)

    # Exemplo de transformação: remover nulos e renomear colunas
    df = df.dropna()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Salvar CSV transformado
    transformed_path = "/tmp/dados_transformados.csv"
    df.to_csv(transformed_path, index=False)
    context['ti'].xcom_push(key='transformed_path', value=transformed_path)

def salvar_no_banco(**context):
    transformed_path = context['ti'].xcom_pull(key='transformed_path')
    df = pd.read_csv(transformed_path)

    # Conectar ao banco (SQLite para simplicidade)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
    conn.close()

# ---------- DEFINIÇÃO DO DAG ----------
with DAG(
    dag_id="pipeline_csv",
    default_args={"owner": "airflow"},
    schedule_interval=None,  # Rodar manualmente
    start_date=days_ago(1),
    catchup=False,
    tags=["exemplo", "etl"],
) as dag:

    tarefa_baixar = PythonOperator(
        task_id="baixar_csv",
        python_callable=baixar_csv,
        provide_context=True,
    )

    tarefa_transformar = PythonOperator(
        task_id="transformar_dados",
        python_callable=transformar_dados,
        provide_context=True,
    )

    tarefa_carregar = PythonOperator(
        task_id="salvar_no_banco",
        python_callable=salvar_no_banco,
        provide_context=True,
    )

    # Definindo ordem de execução
    tarefa_baixar >> tarefa_transformar >> tarefa_carregar
