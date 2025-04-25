import sys
sys.path.append('/opt/airflow')
from scripts.send_email import send_email
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
    dag_id='test_send_email',
    description='send_#2',
    start_date=datetime(2025, 4, 16),
) as dag:
    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_email,
    )