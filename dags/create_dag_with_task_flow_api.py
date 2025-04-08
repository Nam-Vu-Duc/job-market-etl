from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'namvu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='dag_with_task_flow_api',
    description='Jdag_with_task_flow_api',
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily'
)
def test():
    @task(multiple_outputs=True)
    def push_name():
        return {
            'first_name': 'Nam',
            'last_name': 'Vu'
        }

    @task
    def push_age():
        return 20

    @task
    def greet(first_name, last_name, age):
        print(f'hello, my name is {first_name} {last_name}, I am {age} years old')

    name = push_name()
    age = push_age()
    greet(first_name=name['first_name'], last_name=name['last_name'], age=age)

greet_etl = test()