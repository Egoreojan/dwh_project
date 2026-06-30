import io
import pandas as pd
from minio import Minio
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_dwh"

def read_s3_data():
    client = Minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )

    bucket = 'bronze'
    object_name = "api/nyt_2026-06-30.parquet"

    try:
        client.stat_object(bucket, object_name)
        print(f"Файл найден: {object_name}")

        response = client.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()

        # Читаем Parquet
        df = pd.read_parquet(io.BytesIO(data))

        return df

    except Exception as e:
        print(f"Ошибка {e}")

def create_bronze_layer():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with open("/opt/airflow/lib/db/pg/bronze/ddl.create_layer.sql", "r", encoding="utf-8") as file, \
            open("/opt/airflow/lib/db/pg/bronze/ddl.create_layer__check.sql") as file_check:
        ddl_script = file.read()
        ddl_script_check = file_check.read()

    connection = pg_hook.get_conn()
    with connection.cursor() as cur:
        cur.execute(ddl_script)
        connection.commit()

        cur.execute(ddl_script_check)
        result = [row[0] for row in cur.fetchall()]

        if len(result) <= 0:
            connection.rollback()
            raise Exception("Table of bronze layer has not created")






