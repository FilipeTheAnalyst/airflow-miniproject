import json
import logging
import requests
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

def fetch_weather_data(**kwargs):
    city = Variable.get("city", default_var="London")  # Fetch city from Airflow Variables

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

    # Push the fetched data to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='weather_data', value=weather_data)

def store_weather_data(**kwargs):
    ti = kwargs['ti']
    # Fetch the weather data from XCom
    weather_data = ti.xcom_pull(task_ids='fetch_weather_data', key='weather_data')
    if not weather_data:
        logging.error("No weather data found in XCom.")
        raise ValueError("No weather data available to store.")
    try:
        # Connect to DuckDB (creates the database file if it doesn't exist)
        con = duckdb.connect('dags/data/weather.duckdb', read_only=False)
    
        # Create a table for weather data if it doesn't exist.
        con.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                city VARCHAR,
                data JSON
            )
        """)
        # For simplicity, store the entire JSON as a string.
        city = Variable.get("city", default_var="London")  # Fetch city from Airflow Variables
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
    description='A DAG that fetches weather data and stores it in DuckDB',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task: Fetch weather data from a public API.
    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    # Task: Store the fetched weather data into a DuckDB database.
    store_weather = PythonOperator(
        task_id='store_weather_data',
        python_callable=store_weather_data,
        provide_context=True
    )

    # Task: Notify in case of any failure in upstream tasks.
    notify_failure_task = PythonOperator(
        task_id='notify_failure',
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED  # Runs if at least one upstream task fails.
    )

    # Define task dependencies.
    fetch_weather >> store_weather
    [fetch_weather, store_weather] >> notify_failure_task