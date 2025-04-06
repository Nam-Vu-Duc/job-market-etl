from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from gunicorn.workers.ggevent import PyWSGIHandler

default_args = {
    'owner': 'namvu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    name = ti.xcom_pull(task_ids='push_name', key='name')
    age = ti.xcom_pull(task_ids='push_age', key='age')
    print(f'hello, my name is {name}, I am {age} years old')

def push_name(ti):
    ti.xcom_push(key='name', value='Nam')

def push_age(ti):
    ti.xcom_push(key='age', value=20)

with DAG(
    default_args=default_args,
    dag_id='jobs_market_etl',
    description='Jobs Market ETL',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )

    task2 = PythonOperator(
        task_id='push_name',
        python_callable=push_name
    )

    task3 = PythonOperator(
        task_id='push_age',
        python_callable=push_age
    )

    [task2, task3] >> task1