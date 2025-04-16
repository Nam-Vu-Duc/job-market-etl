import psycopg2
import time
import json
from confluent_kafka import Consumer, SerializingProducer, KafkaError

# config kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'jobs-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
topics = ['address_report', 'source_report', 'exp_report']
consumer = Consumer(conf)
consumer.subscribe(topics)

# config postgres connect
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="root",
    host="host.docker.internal",
    port="5432"
)
cur = conn.cursor()

def store_address_data_to_postgres(address_data):
    print('Start storing address data to postgres')
    try:
        # insert to address_report table
        cur.execute(
            """
            INSERT INTO address_report(query_date, address, min_salary, max_salary, avg_salary, total_jobs) 
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_date, address) DO UPDATE SET 
                min_salary = EXCLUDED.min_salary,
                max_salary = EXCLUDED.max_salary,
                avg_salary = EXCLUDED.avg_salary,
                total_jobs = EXCLUDED.total_jobs
            """,
            (
                time.strftime("%Y-%m-%d"),
                address_data.get('address_cleaned'),
                address_data.get('min_salary', None),
                address_data.get('max_salary', None),
                address_data.get('avg_salary', None),
                address_data.get('total_jobs', None)
            )
        )
        conn.commit()
    except Exception as e:
        print(e)

def store_source_data_to_postgres(source_data):
    print('Start storing source data to postgres')
    try:
        # insert to source_report table
        cur.execute(
            """
            INSERT INTO source_report(query_date, source, min_salary, max_salary, avg_salary, total_jobs) 
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_date, source) DO UPDATE SET
                min_salary = EXCLUDED.min_salary,
                max_salary = EXCLUDED.max_salary,
                avg_salary = EXCLUDED.avg_salary,
                total_jobs = EXCLUDED.total_jobs
            """,
            (
                time.strftime("%Y-%m-%d"),
                source_data.get('source'),
                source_data.get('min_salary', None),
                source_data.get('max_salary', None),
                source_data.get('avg_salary', None),
                source_data.get('total_jobs', None)
            )
        )
        conn.commit()
    except Exception as e:
        print(e)

def store_exp_data_to_postgres(exp_data):
    print('Start storing exp data to postgres')
    try:
        # insert to exp_report table
        cur.execute(
            """
            INSERT INTO exp_report(query_date, exp, min_salary, max_salary, avg_salary, total_jobs) 
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_date, exp) DO UPDATE SET 
                min_salary = EXCLUDED.min_salary,
                max_salary = EXCLUDED.max_salary,
                avg_salary = EXCLUDED.avg_salary,
                total_jobs = EXCLUDED.total_jobs
            """,
            (
                time.strftime("%Y-%m-%d"),
                exp_data.get('final_exp'),
                exp_data.get('min_salary', None),
                exp_data.get('max_salary', None),
                exp_data.get('avg_salary', None),
                exp_data.get('total_jobs', None)
            )
        )
        conn.commit()
    except Exception as e:
        print(e)

def fetch_from_kafka_and_store_to_postgres():
    try:
        while True:
            msg = consumer.poll(2.0)  # Wait up to 1 second
            if msg is None:
                continue
            if msg.error():
                print("Error:", msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                if msg.topic() == 'address_report':
                    store_address_data_to_postgres(data)
                elif msg.topic() == 'source_report':
                    store_source_data_to_postgres(data)
                else:
                    store_exp_data_to_postgres(data)

    except KeyboardInterrupt:
        print("Stopped.")

    finally:
        consumer.close()
        return