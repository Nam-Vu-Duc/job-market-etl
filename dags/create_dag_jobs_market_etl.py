import sys
import os

# Add the parent directory (webScraping) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now use an absolute import
from scripts import scrape
from scripts import process
from scripts import visualize

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