from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging
from python_scripts.telegram_notification import on_success_callback, on_failure_callback


# Загрузка данных из Minio в Postgres

def load_folder_to_postgres(folder: str, **context):

    # Загружаем все JSON-файлы из указанной папки в таблицу с тем же именем

    hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'raw-data-bucket'
    minio_client = hook.get_conn()

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    # получаем уже обработанные ключи файлов
    with engine.connect() as conn:
        result = conn.execute("SELECT file_key FROM raw.processed_files")
        processed_keys = {row[0] for row in result}

    # получаем список файлов в папке
    response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder}/")

    for obj in response.get('Contents', []):
        filekey = obj['Key']

        if filekey in processed_keys:
            continue # пропустить уже обработанный файл

        # читаем объект из Minio
        obj_data = minio_client.get_object(Bucket=bucket_name, Key=filekey)
        df = pd.read_json(obj_data['Body'])

        # грузим в соответствующую таблицу Postgres
        df.to_sql(folder, engine, schema='raw', if_exists='append', index=False)

        # добавляем ключ в таблицу processed_files
        with engine.begin() as conn:
            conn.execute(
                "INSERT INTO raw.processed_files (file_key) VALUES (%s) ON CONFLICT DO NOTHING",
                (filekey,)
            )

        logging.info(f"Файл {filekey} загружен в raw.{folder}")


with DAG(
    'LOAD_raw_data_to_postgres',
    description='Загрузка JSON-данных из папок Minio в таблицы Postgres',
    start_date=datetime(2025, 8, 15),
    schedule_interval='* * * * *',
    catchup=False,
    tags=['raw'],
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
    max_active_tasks=2
) as dag:

    # список папок в Minio, соответствующих названиям таблиц в Postgres
    FOLDERS = ['users', 'sessions', 'events', 'orders', 'campaigns', 'userCampaigns']

    for folder in FOLDERS:
        PythonOperator(
            task_id=f"load_{folder}",
            python_callable=load_folder_to_postgres,
            op_kwargs={"folder": folder},
        )