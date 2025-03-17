from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
import logging
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

aws_conn = BaseHook.get_connection("aws_default")  # Replace with your actual connection ID
S3_BUCKET = "daily-nurse-staffing-data"  # Set your S3 bucket name
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password
SNOWFLAKE_CONN_ID = "snowflake-conn"

# test AWS connection
def test_aws_connection():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='us-west-2'  # Replace with your region if needed
    )
    
    try:
        response = s3_client.list_buckets()
        print("S3 Buckets:", response['Buckets'])
    except Exception as e:
        print("Error connecting to AWS S3:", str(e))
        raise

def create_spark_session():
    spark = SparkSession.builder \
        .appName("SparkS3Test") \
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.7.3",
        ) \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.region", "us-west-2") \
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        ) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    return spark

def test_s3_connection():
    try:
        spark = create_spark_session()
        test_path = f"s3a://{S3_BUCKET}/"
        df = spark.read.text(test_path)
        if df.isEmpty():
            print("Successfully connected to S3, but no files found in the folder.")
        else:
            print("Successfully connected to S3 and files were found.")
    except Exception as e:
        print(f"Failed to connect to S3: {e}")

    finally:
        if spark:
            spark.stop()

with DAG(
    'test_aws_connection_dag',
    description='Test AWS connection using Airflow',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2025, 3, 8),
    catchup=False,
) as dag:

    test_aws = PythonOperator(
        task_id='test_aws_connection',
        python_callable=test_aws_connection,
    )
    test_s3_spark = PythonOperator(
        task_id='test_s3_connection',
        python_callable=test_s3_connection,
    )
    test_snowflake = SnowflakeOperator(
        task_id="test_snowflake_connection",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT CURRENT_VERSION();",
    )

test_aws >> test_s3_spark >> test_snowflake
