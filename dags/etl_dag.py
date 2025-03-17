from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'etl_dag',
    default_args=default_args,
    description='Extract staffing data from API, convert to Parquet and store in S3.',
    schedule_interval='0 0 1 * *',
    catchup=False,
) as dag:

    fetch_data_task = BashOperator(
        task_id="extract_data",
        bash_command="python /opt/airflow/dags/scripts/extract_data.py",
    )
