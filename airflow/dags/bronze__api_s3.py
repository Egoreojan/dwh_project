import io
from datetime import datetime, timedelta
from logging import exception
from operator import index

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import requests
import pandas as pd

MINIO_S3_CONNECT_NAME = 'S3_mino'
API_HOST_VAR = 'API_HOST'
API_KEY_VAR = 'API_KEY'
API_ROUTE = 'viewed/1.json'

def load_raw_data():
    # Get variables
    api_host = Variable.get(API_HOST_VAR) + API_ROUTE
    api_key = Variable.get(API_KEY_VAR)

    try:
        # Get data from api
        response = requests.get(
            api_host,
            params={'api-key': api_key}
        )

        if response.status_code != 200:
            exception(f"Response code is not success: {response.status_code}")

        data = pd.DataFrame(response.json())
        if data is None or data.empty:
            exception("Empty data!")

        # Save data to S3
        buffer_json = io.BytesIO()
        data.to_json(buffer_json, index=False)


        buffer_json.seek(0)

    except Exception as e:
        print(e)


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
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


