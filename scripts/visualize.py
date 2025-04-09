import psycopg2
from confluent_kafka import Consumer, SerializingProducer, KafkaError

conf = {'bootstrap.servers': 'localhost:9092'}

consumer = Consumer(conf | {
    'group.id': 'jobs-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe(['jobs-topic'])

producer = SerializingProducer(conf)

def create_tables(conn, cur):
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

def fetch_from_postgres(conn, cur):
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

def visualize():
    conn = psycopg2.connect('host=localhost dbname=voting user=postgres password=postgres')
    cur = conn.cursor()

    create_tables(conn, cur)
    source_report, address_report, exp_report = fetch_from_postgres(conn, cur)
    print(source_report, address_report, exp_report)