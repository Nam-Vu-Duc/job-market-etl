import mysql.connector
import psycopg2
from confluent_kafka.admin import AdminClient, NewTopic

def create_mysql_tables():
    try:
        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="root",
            password="root"
        )
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs.jobs (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                position    VARCHAR(255),
                company     VARCHAR(255),
                address     VARCHAR(255),
                source      VARCHAR(255),
                query_day   DATE,
                min_salary  FLOAT,
                max_salary  FLOAT,
                experience  INT
            )
            """
        )
        conn.commit()
        print('Create mysql successfully')
    except Exception as e:
        print(e)

def create_postgres_tables():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="root",
            host="host.docker.internal",
            port="5432"
        )
        cur = conn.cursor()

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
        print('create postgresql successfully')
    except Exception as e:
        print(e)

def create_kafka_topics():
    try:
        conf = {'bootstrap.servers': 'broker:29092'}
        admin_client = AdminClient(conf)

        jobs_topic      = NewTopic('jobs-topic', num_partitions=2, replication_factor=1)
        address_report  = NewTopic('address_report', num_partitions=2, replication_factor=1)
        source_report   = NewTopic('source_report', num_partitions=2, replication_factor=1)
        exp_report      = NewTopic('exp_report', num_partitions=2, replication_factor=1)

        futures = admin_client.create_topics([jobs_topic, address_report, source_report, exp_report])

        for topic, future in futures.items():
            try:
                future.result()  # This will raise an exception if topic creation fails
                print(f"✅ Topic '{topic}' created successfully.")
            except Exception as e:
                print(f"⚠️ Failed to create topic '{topic}': {e}")
    except Exception as e:
        print(e)

def initial_requirements():
    create_mysql_tables()
    create_postgres_tables()
    create_kafka_topics()