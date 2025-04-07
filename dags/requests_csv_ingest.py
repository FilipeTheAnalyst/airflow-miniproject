from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import requests
import pandas as pd
from io import StringIO


# URL of the CSV to fetch
CSV_URL = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_players.csv"
OUTPUT_PATH = '/opt/airflow/dags/data/atp_players.csv'

# Task 1: Download CSV content using requests
def download_csv(**kwargs):
    logging.info(f"Downloading CSV file from {CSV_URL}")
    response = requests.get(CSV_URL)
    response.raise_for_status()  # Raise exception for HTTP errors
    csv_text = response.text
    kwargs['ti'].xcom_push(key='csv_data', value=csv_text)

    logging.info("CSV file downloaded successfully")

# Task 2: Save the CSV content to a file
def save_csv(**kwargs):
    csv_text = kwargs['ti'].xcom_pull(key='csv_data', task_ids='download_csv')
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as file:
        file.write(csv_text)
    logging.info(f"CSV data saved to {OUTPUT_PATH}")
    kwargs['ti'].xcom_push(key='file_path', value=OUTPUT_PATH)

# Task 3: Read the CSV file using pandas
def read_csv(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='save_csv')
    df = pd.read_csv(file_path)
    logging.info(f"CSV has {len(df)} rows")
    logging.info(df.head().to_string())

default_args = {
    'owner': 'airflow'
}

# Define the Airflow DAG
with DAG(
    dag_id='fetch_atp_players_with_requests',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['csv', 'requests', 'tennis']
) as dag:

    download_csv = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
        provide_context=True
    )

    save_csv = PythonOperator(
        task_id='save_csv',
        python_callable=save_csv,
        provide_context=True
    )

    read_csv = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        provide_context=True
    )

    download_csv >> save_csv >> read_csv