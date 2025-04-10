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
conn = psycopg2.connect('host=localhost dbname=voting user=postgres password=postgres')
cur = conn.cursor()

def create_postgres_tables(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS source_report (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            query_date date,
            source varchar(255),
            min_salary float,
            max_salary float,
            total_jobs integer
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS address_report (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            query_date date,
            address varchar(255),
            min_salary float,
            max_salary float,
            total_jobs integer
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS exp_report (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            query_date date,
            exp INTEGER,
            min_salary float,
            max_salary float,
            total_jobs integer
        )
        """
    )
    conn.commit()

def fetch_from_kafka():
    address_data, source_data, exp_data = [], [], []
    try:
        while True:
            msg = consumer.poll(1.0)  # Wait up to 1 second
            if msg is None:
                continue
            if msg.error():
                print("Error:", msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"[{msg.topic()}] {data}")
                if msg.topic() == 'address_report':
                    address_data.append(data)
                elif msg.topic() == 'source_report':
                    source_data.append(data)
                else:
                    exp_data.append(data)

    except KeyboardInterrupt:
        print("Stopped.")

    finally:
        consumer.close()
        return address_data, source_data, exp_data

def store_to_postgres(address_data, source_data, exp_data):
    try:
        # insert to address_report table
        cur.executemany(
            """
            INSERT INTO address_report(query_date, address, min_salary, max_salary, avg_salary, total_jobs) 
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_date, address) DO UPDATE SET 
                min_salary = EXCLUDED.min_salary,
                max_salary = EXCLUDED.max_salary,
                avg_salary = EXCLUDED.avg_salary,
                total_jobs = EXCLUDED.total_jobs
            """,
            [
                (
                    time.strftime("%Y-%m-%d"),
                    d.get('address'),
                    d.get('min_salary', None),
                    d.get('max_salary', None),
                    d.get('avg_salary', None),
                    d.get('total_jobs', None)
                )
                for d in address_data
            ]
        )

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
            [
                (
                    time.strftime("%Y-%m-%d"),
                    d.get('source'),
                    d.get('min_salary', None),
                    d.get('max_salary', None),
                    d.get('avg_salary', None),
                    d.get('total_jobs', None)
                )
                for d in source_data
            ]
        )

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
            [
                (
                    time.strftime("%Y-%m-%d"),
                    d.get('exp'),
                    d.get('min_salary', None),
                    d.get('max_salary', None),
                    d.get('avg_salary', None),
                    d.get('total_jobs', None)
                )
                for d in exp_data
            ]
        )
        conn.commit()
    except Exception as e:
        print(e)

def fetch_from_postgres():
    cur.execute("""
        select * from source_report
    """)
    source_report = cur.fetchone()

    cur.execute("""
        select * from address_report
    """)
    address_report = cur.fetchone()

    cur.execute("""
        select * from exp_report
    """)
    exp_report = cur.fetchone()

    return source_report, address_report, exp_report

def visualize_by_superset():
    return

def visualize():
    address_data, source_data, exp_data = fetch_from_kafka()
    store_to_postgres(address_data, source_data, exp_data)
