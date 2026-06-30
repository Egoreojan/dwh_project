from datetime import date, timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.bronze_s3_postgres import create_bronze_layer

today = date.today()

default_args = {
    "owner": "airflow",
    "retries_delay": timedelta(seconds=10),
    "start_date": datetime(today.year, today.month, today.day),
    "catchup": False,
    "execution_timeout": timedelta(seconds=20),
    "schedule": "@daily",
    "max_active_runs": 1
}

with DAG(
    dag_id="bronze__input_data",
    default_args=default_args
) as dag:

    create_dwh_layer = PythonOperator(
        task_id="create_dwh_layer",
        python_callable=create_bronze_layer
    )