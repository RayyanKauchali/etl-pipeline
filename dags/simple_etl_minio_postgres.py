# dags/simple_etl_minio_postgres.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, sys

# Ensure project root on path if Airflow workers need it
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from etl_tasks import run_etl_from_minio_to_postgres

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_etl_minio_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "minio", "postgres"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl_from_minio_to_postgres",
        python_callable=lambda: run_etl_from_minio_to_postgres("sample/orders_sample.csv"),
    )
