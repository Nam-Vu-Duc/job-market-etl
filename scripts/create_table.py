import mysql.connector
import psycopg2

def create_mysql_tables():
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

def create_postgres_tables():
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

def create_table():
    create_mysql_tables()
    create_postgres_tables()