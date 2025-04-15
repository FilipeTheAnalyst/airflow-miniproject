import json
import logging
import requests
import duckdb
import os

from airflow import XComArg
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator, S3ListOperator
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import BranchPythonOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier

WEATHER_CITIES = os.environ.get("CITIES")
S3_INGEST_BUCKET = os.environ.get("S3_INGEST_BUCKET")
SLACK_MESSAGE = """
Hello! The {{ ti.task_id }} task is saying hi :wave: 
Today is the {{ ds }} and this task finished with the state: {{ ti.state }} :tada:.
"""
SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")

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
        return json.loads(WEATHER_CITIES)

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
        bucket = S3_INGEST_BUCKET
        city = weather_obj["city"]
        city_key = city.lower().replace(" ", "_")
        key = f"weather_data/{city_key}.json"
        data = json.dumps(weather_obj["data"])
        logging.info(f"Uploading data for {city} to S3 at {key}")
        s3.load_string(data, key, bucket_name=bucket, replace=True)
        return key
    
    @task
    def collect_s3_keys():
        """Reconstruct the list of expected S3 keys after sensors finish."""
        cities = json.loads(WEATHER_CITIES)
        return [f"weather_data/{city.lower().replace(' ', '_')}.json" for city in cities]


    @task
    def store_to_duckdb(s3_keys: list[str]):
        """Download weather data from S3 and store it in DuckDB."""
        s3 = S3Hook(aws_conn_id="aws_default")
        bucket = S3_INGEST_BUCKET
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
        return "success"

    def branch_decision(**kwargs):
        result = kwargs["ti"].xcom_pull(task_ids="store_to_duckdb")
        return "delete_s3_objects" if result == "success" else "notify_failure"

    branching = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=branch_decision,
        provide_context=True
    )

    # Dummy task to join branch outcomes
    join = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # List all S3 files before deletion
    list_files_ingest_bucket = S3ListOperator(
        task_id="list_files_ingest_bucket",
        aws_conn_id="aws_default",
        bucket=S3_INGEST_BUCKET
    )

    delete_s3_objects = S3DeleteObjectsOperator.partial(
        task_id="delete_s3_objects",
        bucket=S3_INGEST_BUCKET,
        aws_conn_id="aws_default",
    ).expand(keys=XComArg(list_files_ingest_bucket))

    @task(
        on_success_callback=SlackNotifier(
            slack_conn_id="slack_conn",
            text=SLACK_MESSAGE,
            channel="airflow-de-project"
        ),
        trigger_rule=TriggerRule.ONE_FAILED
    )
    def notify_failure():
        logging.warning("One or more tasks in the weather DAG failed!")

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_success():
        try:
            slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
            message = {
                "text": f":white_check_mark: Weather DAG succeeded on {{ ds }}!"
            }
            requests.post(slack_webhook, json=message)
            logging.info("Slack success message sent.")
        except Exception as e:
            logging.error(f"Failed to send Slack success notification: {e}")

    # DAG Execution Flow
    cities = get_cities()
    weather_data = fetch_weather.expand(city=cities)
    s3_keys = upload_to_s3.expand(weather_obj=weather_data)

    # TaskGroup remains unchanged
    with TaskGroup("wait_for_files_group") as wait_for_files_group:
        for city in json.loads(WEATHER_CITIES):
            safe_city = city.lower().replace(" ", "_")
            S3KeySensor(
                task_id=f"wait_for_{safe_city}",
                bucket_name=S3_INGEST_BUCKET,
                aws_conn_id="aws_default",
                poke_interval=10,
                timeout=120,
                soft_fail=False,
                bucket_key=f"weather_data/{safe_city}.json"
            )

    s3_keys_final = collect_s3_keys()
    stored = store_to_duckdb(s3_keys_final)

    # Set dependencies cleanly
    cities >> weather_data >> s3_keys >> wait_for_files_group >> s3_keys_final >> stored >> list_files_ingest_bucket >> branching
    branching >> [delete_s3_objects, notify_failure()] >> join >> notify_success()
    [weather_data, stored, delete_s3_objects] >> notify_failure()


# Instantiate the DAG
weather_dag = weather_pipeline_dynamic()
