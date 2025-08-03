from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from python_scripts.data_generator import generate_all_data
from python_scripts.telegram_notification import on_success_callback, on_failure_callback


with DAG(
    'GENERATE_json_data_dag',
    description='Генерация JSON-данных',
    schedule_interval='* * * * *',
    start_date=datetime(2025, 8, 2),
    catchup=False,
    tags = ['raw'],
    on_failure_callback = on_failure_callback,
    on_success_callback = on_success_callback
) as dag:

    generate_data_task = PythonOperator(
        task_id='generate_data_task',
        python_callable=generate_all_data,
    )

    generate_data_task