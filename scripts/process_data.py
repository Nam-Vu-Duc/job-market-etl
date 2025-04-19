import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
from pyspark.sql import SparkSession
from pyspark.sql.functions import min as _min, max as _max, avg, from_json, count, col, to_json, struct, when, filter, split, explode, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

def process_data():
    print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
    try:
        # Initialize SparkSession
        spark = (SparkSession.builder
            .master("local[*]")
            .appName("JobMarket")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Spark-Kafka integration
            .config("spark.driver.extraClassPath", "C:/Users/admin/PycharmProjects/PythonProject/postgresql-42.7.5.jar")  # PostgresSQL driver
            .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
            .getOrCreate()
        )

        # Prevents schema mismatches
        job_schema = StructType([
            StructField("position"  , StringType() , True),
            StructField("company"   , StringType() , True),
            StructField("address"   , StringType() , True),
            StructField("source"    , StringType() , True),
            StructField("query_day" , DateType()   , True),
            StructField("min_salary", FloatType()  , True),
            StructField("max_salary", FloatType()  , True),
            StructField("final_exp" , IntegerType(), True)
        ])

        # Read data from Kafka 'jobs_topics'
        jobs_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "jobs-topic")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), job_schema).alias("data"))
            .select("data.*")
        )

        # CLEAN ADDRESS DATA
        # Step 1: Replace common separators with a single one, e.g., comma
        jobs_replaced = (
            jobs_df
            .withColumn(
                "address_replaced",
                regexp_replace(col("address"), r"[\n\r&,\-]+", "|")
            )
        )

        # Step 2: Split by comma and explode into multiple rows
        job_split = (
            jobs_replaced
            .withColumn(
                "address_split",
                explode(split("address_replaced", r"\|"))
            )
        )

        # Step 3: Trim whitespace
        job_cleaned = (
            job_split
            .withColumn(
                "address_cleaned",
                trim(col("address_split"))
            )
        )

        # Calc address report
        address_report = (
            job_cleaned
            .withColumn(
                "salary",
                when(col("min_salary") > 0, (col("min_salary") + col("max_salary")) / 2)
                .otherwise(col("max_salary") / 2)
            )
            .groupBy("address_cleaned")
            .agg(
                _min(when(col("min_salary") > 0, col("min_salary"))).alias("min_salary"),
                _max("max_salary").alias("max_salary"),
                avg("salary").alias("avg_salary"),
                count("address").alias("total_jobs")
            )
        )
        address_report_json = address_report.select(to_json(struct("*")).alias("value"))

        # Calc source report
        source_report = (
            job_split
            .withColumn(
                "salary",
                when(col("min_salary") > 0, (col("min_salary") + col("max_salary")) / 2)
                .otherwise(col("max_salary") / 2)
            )
            .groupBy("source")
            .agg(
                _min(when(col("min_salary") > 0, col("min_salary"))).alias("min_salary"),
                _max("max_salary").alias("max_salary"),
                avg("salary").alias("avg_salary"),
                count("source").alias("total_jobs")
            )
        )
        source_report_json = source_report.select(to_json(struct("*")).alias("value"))

        # Calc exp report
        exp_report = (
            job_split
            .withColumn(
                "salary",
                when(col("min_salary") > 0, (col("min_salary") + col("max_salary")) / 2)
                .otherwise(col("max_salary") / 2)
            )
            .groupBy("final_exp")
            .agg(
                _min(when(col("min_salary") > 0, col("min_salary"))).alias("min_salary"),
                _max("max_salary").alias("max_salary"),
                avg("salary").alias("avg_salary"),
                count("final_exp").alias("total_jobs")
            )
        )
        exp_report_json = exp_report.select(to_json(struct("*")).alias("value"))

        # Write aggregated data to Kafka topics
        address_report_to_kafka = (
            address_report_json
            .writeStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("topic", "address_report")
            .option("checkpointLocation", "/opt/airflow/checkpoints/checkpoint2")
            .outputMode("update")
            .start()
        )

        # Write aggregated data to Kafka topics
        source_report_to_kafka = (
            source_report_json
            .writeStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("topic", "source_report")
            .option("checkpointLocation", "/opt/airflow/checkpoints/checkpoint3")
            .outputMode("update")
            .start()
        )

        # Write aggregated data to Kafka topics
        exp_report_to_kafka = (
            exp_report_json
            .writeStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("topic", "exp_report")
            .option("checkpointLocation", "/opt/airflow/checkpoints/checkpoint4")
            .outputMode("update")
            .start()
        )

        # Await termination for the streaming queries
        address_report_to_kafka.awaitTermination()
        source_report_to_kafka.awaitTermination()
        exp_report_to_kafka.awaitTermination()

    except Exception as e:
        print(e)