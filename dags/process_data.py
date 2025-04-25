import sys
sys.path.append('/opt/airflow')
from scripts.process_data import process_data
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
    dag_id='test_process_data',
    description='send_#2',
    start_date=datetime(2025, 4, 16),
) as dag:
    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )