from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "sthefany",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pyspark_datalake_dag",
    default_args=default_args,
    description="Pipeline PySpark diário - COVID Data Lake",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 12),
    catchup=False
) as dag:

    run_pyspark = BashOperator(
        task_id="run_pyspark_datalake",
        bash_command="spark-submit /opt/airflow/scripts/process_datalake.py"
    )

    run_pyspark
