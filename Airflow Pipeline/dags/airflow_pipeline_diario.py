from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

def run_pipeline():
    print("Rodando pipeline...")
    df = pd.read_csv("/caminho/dados.csv")
    conn = sqlite3.connect("/caminho/meu_banco.db")
    df.to_sql("tabela_dados", conn, if_exists="replace", index=False)
    conn.close()

default_args = {
    "owner": "sthefany",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["seu.email@empresa.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_diario",
    default_args=default_args,
    description="Pipeline diário para atualizar banco",
    schedule_interval="0 3 * * *",  # roda às 03h
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    tarefa = PythonOperator(
        task_id="executa_pipeline",
        python_callable=run_pipeline,
    )

    tarefa
