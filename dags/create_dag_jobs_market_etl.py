import sys
sys.path.append('/opt/airflow')
from scripts.create_table import create_table
from scripts.scrape_data import scrape_data
from scripts.process_data import process_data
from scripts.visualize_report import fetch_from_kafka_and_store_to_postgres
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
    dag_id='dag_for_jobs_market_etl',
    description='fixing_deni_chromedriver_#9',
    start_date=datetime(2025, 4, 16),
    # schedule_interval='@daily'
) as dag:
    # create_table = PythonOperator(
    #     task_id='create_table',
    #     python_callable=create_table,
    # )

    scrape_data = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data,
    )

    # process_data = PythonOperator(
    #     task_id='process_data',
    #     python_callable=process_data
    # )
    #
    # visualize_report = PythonOperator(
    #     task_id='visualize_report',
    #     python_callable=fetch_from_kafka_and_store_to_postgres
    # )
    # send_email = PythonOperator(
    #     task_id='send_email',
    #     python_callable=send_email
    # )

    # create_table >> scrape_data >> process_data >> visualize_report >> send_email