import sys
sys.path.append('/opt/airflow')
from scripts.visualize_report import fetch_from_kafka_and_store_to_postgres
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'namvu',
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with DAG(
    default_args=default_args,
    dag_id='test_fetch_from_kafka_and_store_to_postgres',
    description='send_#2',
    start_date=datetime(2025, 4, 16),
) as dag:
    fetch_from_kafka_and_store_to_postgres = PythonOperator(
        task_id='fetch_from_kafka_and_store_to_postgres',
        python_callable=fetch_from_kafka_and_store_to_postgres,
    )