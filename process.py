import psycopg2
import pandas as pd
import simplejson as json
from confluent_kafka import Consumer, SerializingProducer, KafkaError
from scrape import delivery_report
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'jobs-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe(['jobs-topic'])

producer = SerializingProducer(conf)

def process(data) -> None:
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    data = pd.DataFrame(data)
    data.columns = ['position', 'company', 'salary', 'address', 'exp']

    data['source'] = 'topCV'

    # Ensure salary column is a string and stripped of leading/trailing spaces
    data['salary'] = data['salary'].astype(str).str.strip()

    # Initialize columns
    data['min_salary'] = 0
    data['max_salary'] = 0

    # Case 1: 'Thoả thuận' → No salary values
    condition = data['salary'].str.contains('Thoả thuận', na=False)
    data.loc[condition, ['min_salary', 'max_salary']] = [0, 0]

    # Case 2: 'Tới X USD' or 'Tới X triệu'
    condition = data['salary'].str.startswith('Tới')
    is_usd = data['salary'].str.contains('USD', na=False)
    data.loc[condition, 'max_salary'] = data.loc[condition, 'salary'].str.split().str[1].str.replace(',', '').astype(float)
    data.loc[condition & is_usd, 'max_salary'] *= 25000 / 1000000  # Convert USD to VND
    data.loc[condition & is_usd & (data['max_salary'] > 100), 'max_salary'] //= 12

    # Case 3: 'X - Y USD' or 'X - Y triệu'
    condition = data['salary'].str.contains('-')
    split_salaries = data.loc[condition, 'salary'].str.replace(',', '').str.split(' - ')

    data.loc[condition, 'min_salary'] = split_salaries.str[0].astype(float)
    data.loc[condition, 'max_salary'] = split_salaries.str[1].str.split().str[0].astype(float)
    data.loc[condition & is_usd, ['min_salary', 'max_salary']] *= 25000 / 1000000

    print(data)

def store_data():
    conn = psycopg2.connect('host=localhost dbname=jobs user=postgres password=postgres')
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            position VARCHAR(255),
            company VARCHAR(255),
            salary VARCHAR(255),
            address VARCHAR(255),
            exp VARCHAR(255),
        )
        """
    )
    conn.commit()

if __name__ == '__main__':
    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("JobMarket")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Spark-Kafka integration
             .config("spark.driver.extraClassPath", "C:/Users/admin/PycharmProjects/PythonProject/postgresql-42.7.5.jar")  # PostgresSQL driver
             .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
             .getOrCreate())

    # Read data from Kafka 'votes_topic' and process it
    jobs_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "jobs-topic")
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), vote_schema).alias("data"))
            .select("data.*")
            )

    while True:
        msg = consumer.poll(1.0)  # Poll messages with a 1-second timeout
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        print(f"Received: {msg.value().decode('utf-8')}")