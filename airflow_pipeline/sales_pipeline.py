from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import psycopg2

# -----------------------------------------------------
# TASK 1 — Download CSV
# -----------------------------------------------------
def download_csv():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv"
    response = requests.get(url)

    with open("/tmp/data.csv", "wb") as f:
        f.write(response.content)

# -----------------------------------------------------
# TASK 2 — Transform CSV
# -----------------------------------------------------
def transform_csv():
    df = pd.read_csv("/tmp/data.csv")

    # Example transformation:
    df.columns = [col.lower() for col in df.columns]
    df["total"] = df.sum(axis=1, numeric_only=True)

    df.to_csv("/tmp/data_transformed.csv", index=False)

# -----------------------------------------------------
# TASK 3 — Load into Database
# -----------------------------------------------------
def load_to_database():
    df = pd.read_csv("/tmp/data_transformed.csv")

    conn = psycopg2.connect(
        host="localhost",
        database="mydatabase",
        user="username",
        password="password"
    )
    cur = conn.cursor()

    # Create table
    cur.execute("""
        DROP TABLE IF EXISTS airtravel_table;
        CREATE TABLE airtravel_table (
            month TEXT,
            jan INTEGER,
            feb INTEGER,
            mar INTEGER,
            ...,
            total INTEGER
        );
    """)

    # Insert data
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO airtravel_table VALUES (%s, %s, %s, %s, %s)",
            tuple(row)
        )

    conn.commit()
    conn.close()

# -----------------------------------------------------
# DAG Definition
# -----------------------------------------------------
with DAG(
    dag_id="airflow_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_download_csv = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    task_transform_csv = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_csv,
    )

    task_load_db = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_database,
    )

    # Task order
    task_download_csv >> task_transform_csv >> task_load_db