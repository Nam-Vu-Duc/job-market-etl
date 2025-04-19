import sys
sys.path.append('/opt/airflow')
from scripts.initial_requirements import initial_requirements
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
    dag_id='test_initial_requirements',
    description='send_#2',
    start_date=datetime(2025, 4, 16),
    # schedule_interval='@daily'
) as dag:
    create_table = PythonOperator(
        task_id='initial_requirements',
        python_callable=initial_requirements,
    )