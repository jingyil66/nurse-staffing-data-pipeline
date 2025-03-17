from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake-conn"

with open("/opt/airflow/dags/scripts/s3_to_snowflake.sql", "r") as file:
    sql_query = file.read()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    's3_to_sf_dag',
    default_args=default_args,
    description='Copy data from s3 to snowflake.',
    schedule_interval='0 0 1 * *',
    catchup=False,
) as dag:

    s3_to_snowflake = SnowflakeOperator(
        task_id="s3_to_snowflake",
        sql=sql_query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )