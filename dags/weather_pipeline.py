import json
import logging
import requests
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator, S3ListOperator, S3DeleteObjectsOperator
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_INGEST_BUCKET = Variable.get("s3_ingest_bucket")
S3_OBJECT_KEY = "weather_data.json"

def fetch_weather_data(**kwargs):
    city = Variable.get("city", default_var="London")

    try:
        url = f'https://wttr.in/{city}?format=j1'
        logging.info(f"Fetching weather data for %s from %s", city, url)
        response = requests.get(url)
        response.raise_for_status()
        weather_data = response.json()
        logging.info("Weather data fetched successfully.")
    except Exception as e:
        logging.error(f"Error fetching weather data: %s", e)
        raise

    kwargs['ti'].xcom_push(key='weather_data', value=json.dumps(weather_data))

def store_weather_data(**kwargs):
    ti = kwargs['ti']
    s3_keys = ti.xcom_pull(task_ids='list_s3_weather_files')

    if not s3_keys:
        logging.error("No files found in S3 to process.")
        raise ValueError("No files found in S3.")

    s3 = S3Hook(aws_conn_id='aws_default')
    bucket_name = S3_INGEST_BUCKET

    key = s3_keys[0]
    logging.info(f"Downloading weather data file from S3: {key}")
    file_content = s3.read_key(key, bucket_name=bucket_name)
    weather_data = json.loads(file_content)

    try:
        con = duckdb.connect('dags/data/weather.duckdb', read_only=False)
        con.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                city VARCHAR,
                data JSON
            )
        """)
        city = Variable.get("city", default_var="London")
        con.execute(
            "INSERT INTO weather (city, data) VALUES (?, ?)",
            (city, json.dumps(weather_data))
        )
        logging.info("Weather data stored successfully in DuckDB.")
    except Exception as e:
        logging.error("Error storing weather data in DuckDB: %s", e)
        raise

def notify_failure(**kwargs):
    logging.warning("One or more tasks in the weather pipeline DAG failed. Please check the logs for details.")

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='A DAG that fetches weather data, stores it in S3 and DuckDB, and cleans up S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    store_weather_s3 = S3CreateObjectOperator(
        task_id='store_weather_s3',
        s3_bucket=S3_INGEST_BUCKET,
        s3_key=S3_OBJECT_KEY,
        data="{{ task_instance.xcom_pull(task_ids='fetch_weather_data', key='weather_data') }}",
        replace=True
    )

    list_s3_weather_files = S3ListOperator(
        task_id='list_s3_weather_files',
        bucket=S3_INGEST_BUCKET,
        prefix='weather_data'
    )

    store_weather = PythonOperator(
        task_id='store_weather_data',
        python_callable=store_weather_data,
        provide_context=True
    )

    delete_s3_weather_files = S3DeleteObjectsOperator(
        task_id='delete_s3_weather_files',
        bucket=S3_INGEST_BUCKET,
        keys="{{ task_instance.xcom_pull(task_ids='list_s3_weather_files') }}",
    )

    notify_failure_task = PythonOperator(
        task_id='notify_failure',
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Define DAG dependencies
    fetch_weather >> store_weather_s3 >> list_s3_weather_files
    list_s3_weather_files >> store_weather >> delete_s3_weather_files
    [store_weather_s3, store_weather] >> notify_failure_task