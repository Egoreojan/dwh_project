import io
import logging
import requests
import pandas as pd
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException

MINIO_S3_CONNECT_NAME = 'S3_mino'
API_HOST_VAR = 'API_HOST'
API_KEY_VAR = 'API_KEY'
API_ROUTE = 'viewed/1.json'
FOLDER_PATH_S3 = 'api'

def get_data():
    # Get variables
    api_host = Variable.get(API_HOST_VAR) + API_ROUTE
    api_key = Variable.get(API_KEY_VAR)

    # Get data from api
    response = requests.get(
        api_host,
        params={'api-key': api_key}
    )

    if response.status_code != 200:
        raise AirflowFailException(f"Response code is not success: {response.status_code}")

    logging.log(1, f"Response has got")

    return pd.json_normalize(response.json()['results'])


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    return data[
        ['url', 'id', 'asset_id', 'source', 'published_date', 'updated', 'section', 'title', 'abstract']
    ]

def save_data_to_s3(data: pd.DataFrame, ts):
    # Save data to S3
    buffer_json = io.BytesIO()
    buffer_parquet = io.BytesIO()

    data.to_json(buffer_json)
    data.to_parquet(buffer_parquet, index=False)

    buffer_json.seek(0)
    buffer_parquet.seek(0)

    json_filename = f"{FOLDER_PATH_S3}/nyt_{ts}.json"
    parquet_filename = f"{FOLDER_PATH_S3}/nyt_{ts}.parquet"

    hook_s3 = S3Hook(aws_conn_id="S3_mino")

    hook_s3.load_bytes(
        bytes_data=buffer_json.read(),
        key=json_filename,
        bucket_name="bronze",
        replace=True
    )

    hook_s3.load_bytes(
        bytes_data=buffer_parquet.read(),
        key=parquet_filename,
        bucket_name="bronze",
        replace=True
    )

    buffer_json.close()
    buffer_parquet.close()

    logging.log(1, f"Load to s3 complited")


def load_raw_data(**context):
    try:
        data = get_data()

        if data is None or data.empty:
            raise AirflowFailException("Empty data!")

        data = transform_data(data)
        save_data_to_s3(data, context['ts'])

    except Exception as e:
        logging.exception(f"Error: {e}")
        raise