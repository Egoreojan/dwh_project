from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.bronze_api_s3 import load_raw_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries_delay': timedelta(seconds=10),
    'start_date': datetime(2026, 6, 20),
    'catchup': False,
    'execution_timeout': timedelta(minutes=2),
    'schedule': '@daily'
}

with DAG(
    dag_id='bronze__api_s3',
    default_args=default_args
) as dag:

    upload_data_from_api_to_s3 = PythonOperator(
        task_id="upload_data_from_api_to_s3",
        python_callable=load_raw_data
    )


