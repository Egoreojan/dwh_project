from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.bronze_api_s3 import load_raw_data

today = date.today()

default_args = {
    'owner': 'airflow',
    'retries_delay': timedelta(seconds=10),
    'start_date': datetime(today.year, today.month, today.day),
    'catchup': False,
    'execution_timeout': timedelta(seconds=20),
    'schedule': '@daily',
    'max_active_runs': 1
}

with DAG(
    dag_id='bronze__api_s3',
    default_args=default_args
) as dag:

    upload_data_from_api_to_s3 = PythonOperator(
        task_id="upload_data_from_api_to_s3",
        python_callable=load_raw_data
    )


