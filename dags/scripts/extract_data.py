from pyspark.sql import SparkSession
import requests
import json
from pyspark import SparkContext
from pyspark.sql import Row
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging
from pyspark.sql.functions import col, when
from pyspark.sql.functions import to_timestamp

aws_conn = BaseHook.get_connection("aws_default")  # Replace with your actual connection ID
S3_BUCKET = "daily-nurse-staffing-data"  # Set your S3 bucket name
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password

spark = SparkSession.builder \
    .appName("PBJ Data Ingestion") \
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

if spark is None:
    print("Failed to create Spark session!")
else:
    print("Spark session created successfully!")

def fetch_data_from_api(offset, size=5000):
    url = f"https://data.cms.gov/data-api/v1/dataset/dcc467d8-5792-4e5d-95be-04bf9fc930a1/data?size={size}&offset={offset}"
    response = requests.get(url)
    data = response.json()
    return data

def get_offset_list():
    offset = 1300000
    size = 5000
    offset_list = list(range(0, offset + size, size))
    
    while True:
        data = fetch_data_from_api(offset, size)
        if not data:
            break
        
        offset_list.append(offset)
        
        if len(data) < size:
            break
        
        offset += size
    
    return offset_list

def fetch_data_partition(offset_iter):
    """Make an API request for all offsets of a partition and return an iterator."""
    results = []
    for offset in offset_iter:
        results.extend(fetch_data_from_api(offset))
    return iter(results)

def process_and_store_data():
    """Get data and store it in S3."""
    logging.info("Fetching offset list...")
    offset_list = get_offset_list()
    logging.info(f"Fetched offset list: {offset_list}")

    logging.info("Fetching data in parallel using mapPartitions...")
    rdd = spark.sparkContext.parallelize(offset_list, numSlices=10)
    json_rdd = rdd.mapPartitions(fetch_data_partition)

    logging.info("Converting to DataFrame...")
    df = spark.read.option("mode", "DROPMALFORMED").json(json_rdd)

    # Change Data Type
    logging.info("Changing Data Type")

    df = df.selectExpr(
        "cast(PROVNUM as STRING)",
        "cast(PROVNAME as STRING)",
        "cast(CITY as STRING)",
        "cast(STATE as STRING)",
        "cast(COUNTY_NAME as STRING)",
        "cast(COUNTY_FIPS as INT)",
        "cast(CY_Qtr as STRING)",
        "to_timestamp(WorkDate, 'yyyyMMdd') as WorkDate",
        "cast(MDScensus as INT)",
        "cast(Hrs_RNDON as FLOAT)",
        "cast(Hrs_RNDON_emp as FLOAT)",
        "cast(Hrs_RNDON_ctr as FLOAT)",
        "cast(Hrs_RNadmin as FLOAT)",
        "cast(Hrs_RNadmin_emp as FLOAT)",
        "cast(Hrs_RNadmin_ctr as FLOAT)",
        "cast(Hrs_RN as FLOAT)",
        "cast(Hrs_RN_emp as FLOAT)",
        "cast(Hrs_RN_ctr as FLOAT)",
        "cast(Hrs_LPNadmin as FLOAT)",
        "cast(Hrs_LPNadmin_emp as FLOAT)",
        "cast(Hrs_LPNadmin_ctr as FLOAT)",
        "cast(Hrs_LPN as FLOAT)",
        "cast(Hrs_LPN_emp as FLOAT)",
        "cast(Hrs_LPN_ctr as FLOAT)",
        "cast(Hrs_CNA as FLOAT)",
        "cast(Hrs_CNA_emp as FLOAT)",
        "cast(Hrs_CNA_ctr as FLOAT)",
        "cast(Hrs_NAtrn as FLOAT)",
        "cast(Hrs_NAtrn_emp as FLOAT)",
        "cast(Hrs_NAtrn_ctr as FLOAT)",
        "cast(Hrs_MedAide as FLOAT)",
        "cast(Hrs_MedAide_emp as FLOAT)",
        "cast(Hrs_MedAide_ctr as FLOAT)"
    )

    numerator_columns = ['Hrs_RNDON', 'Hrs_RNDON_emp', 'Hrs_RNDON_ctr',
        'Hrs_RNadmin', 'Hrs_RNadmin_emp', 'Hrs_RNadmin_ctr',
        'Hrs_RN', 'Hrs_RN_emp', 'Hrs_RN_ctr',
        'Hrs_LPNadmin', 'Hrs_LPNadmin_emp', 'Hrs_LPNadmin_ctr',
        'Hrs_LPN', 'Hrs_LPN_emp', 'Hrs_LPN_ctr',
        'Hrs_CNA', 'Hrs_CNA_emp', 'Hrs_CNA_ctr',
        'Hrs_NAtrn', 'Hrs_NAtrn_emp', 'Hrs_NAtrn_ctr',
        'Hrs_MedAide', 'Hrs_MedAide_emp', 'Hrs_MedAide_ctr']
    denominator_column = "MDScensus"

    for num_col in numerator_columns:
        ratio_col_name = f"{num_col}_MDS_Ratio"
        df = df.withColumn(
            ratio_col_name,
            when(col(denominator_column) > 0, col(num_col) / col(denominator_column)).otherwise(None)
        )

    logging.info(f"Created DataFrame with {df.count()} rows.")

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3a://{S3_BUCKET}/staffing_data{current_time}.parquet"

    logging.info(f"Saving to S3: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logging.info("Data successfully written to S3.")

if __name__ == "__main__":
    process_and_store_data()