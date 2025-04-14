import sys
sys.path.append('/opt/airflow')  # Add the parent directory of scripts/, not scripts/ itself
from scripts.scrape_data import scrape
from scripts.process_data import process
from scripts.visualize_report import visualize

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'namvu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_for_jobs_market_etl',
    description='dag_for_jobs_market_etl',
    start_date=datetime(2025, 4, 8),
    schedule_interval='@daily'
) as dag:
    task_scrape = PythonOperator(
        task_id='scrape',
        python_callable=scrape,
    )

    task_process = PythonOperator(
        task_id='process',
        python_callable=process
    )

    task_visualize = PythonOperator(
        task_id='visualize',
        python_callable=visualize
    )

    task_scrape >> task_process >> task_visualize