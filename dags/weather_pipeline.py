import json
import logging
import requests
import duckdb

from airflow import DAG, XComArg
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator, S3ListOperator
)

@dag(
    dag_id="weather_pipeline_dynamic",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    description="Fetch weather data for multiple cities using dynamic task mapping, store in S3 and DuckDB, and clean up S3",
)
def weather_pipeline_dynamic():

    @task
    def get_cities():
        """Fetch the list of cities from Airflow Variable."""
        return json.loads(Variable.get("city", default_var='["London"]'))

    @task
    def fetch_weather(city: str):
        """Fetch weather data for a given city from wttr.in API."""
        try:
            url = f'https://wttr.in/{city}?format=j1'
            logging.info(f"Fetching weather data for {city} from {url}")
            response = requests.get(url)
            response.raise_for_status()
            weather_data = response.json()
            logging.info(f"Successfully fetched data for {city}")
            return {"city": city, "data": weather_data}
        except Exception as e:
            logging.error(f"Error fetching data for {city}: {e}")
            raise

    @task
    def upload_to_s3(weather_obj: dict):
        """Upload the weather data for a city to S3."""
        s3 = S3Hook(aws_conn_id="aws_default")
        bucket = Variable.get("s3_ingest_bucket")
        city = weather_obj["city"]
        key = f"weather_data/{city.lower()}.json"
        data = json.dumps(weather_obj["data"])
        logging.info(f"Uploading data for {city} to S3 at {key}")
        s3.load_string(data, key, bucket_name=bucket, replace=True)
        return key

    @task
    def store_to_duckdb(s3_keys: list[str]):
        """Download weather data from S3 and store it in DuckDB."""
        s3 = S3Hook(aws_conn_id="aws_default")
        bucket = Variable.get("s3_ingest_bucket")
        con = duckdb.connect("dags/data/weather.duckdb", read_only=False)

        con.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                city VARCHAR,
                data JSON
            )
        """)

        for key in s3_keys:
            content = s3.read_key(key, bucket_name=bucket)
            city = key.split("/")[-1].replace(".json", "")
            con.execute("INSERT INTO weather (city, data) VALUES (?, ?)", (city, content))

        logging.info("Stored weather data in DuckDB for all cities.")

    # fetch the file names from the ingest S3 bucket
    list_files_ingest_bucket = S3ListOperator(
        task_id="list_files_ingest_bucket",
        aws_conn_id="aws_default",
        bucket=Variable.get("s3_ingest_bucket")
    )

    delete_s3_objects = S3DeleteObjectsOperator.partial(
        task_id="delete_s3_objects",
        bucket=Variable.get("s3_ingest_bucket"),
        aws_conn_id="aws_default",
    ).expand(keys=XComArg(list_files_ingest_bucket))

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_failure():
        """Notify if any upstream task fails."""
        logging.warning("One or more tasks in the weather DAG failed!")

    # DAG Execution Flow
    cities = get_cities()
    weather_data = fetch_weather.expand(city=cities)
    s3_keys = upload_to_s3.expand(weather_obj=weather_data)
    stored = store_to_duckdb(s3_keys)

    # Failure handling
    [weather_data, stored, list_files_ingest_bucket, delete_s3_objects] >> notify_failure()

    # set dependencies for tasks not set by the TaskFlowAPI
    s3_keys >> list_files_ingest_bucket >> stored >> delete_s3_objects

# Instantiate the DAG
weather_dag = weather_pipeline_dynamic()