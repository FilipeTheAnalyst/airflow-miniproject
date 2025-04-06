import csv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def read_csv(**kwargs):
    data = []
    try:
        file_path = '/opt/airflow/dags/data/sample.csv'

        with open(file_path, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)

        logging.info("CSV data read successfully: %s", data)
    except Exception as e:
        logging.error("Error reading CSV from %s: %s", file_path, e)
        raise

    # Push CSV data to XCom so that it can be used by downstream tasks
    kwargs['ti'].xcom_push(key='csv_data', value=data)

def process_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='csv_data', task_ids='read_csv_task')
    if not data:
        logging.info("No CSV data received from previous task")
        return

    # Example processing: Count the number of rows
    row_count = len(data)
    logging.info("Processing CSV data: Found %d rows", row_count)

    # Additional processing logic can be added here

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='csv_ingest',
    default_args=default_args,
    description='A DAG that reads and processes CSV data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: List files in the 'data' directory to ensure our CSV is in place
    list_files = BashOperator(
        task_id='list_files',
        bash_command='ls -l /opt/airflow/dags/data'
    )

    # Task 2: Read the CSV file and push its content via XCom
    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
        provide_context=True
    )

    # Task 3: Process the CSV data received from the previous task
    process_csv_task = PythonOperator(
        task_id='process_csv_task',
        python_callable=process_csv,
        provide_context=True
    )

    # Define task dependencies: list_files -> read_csv_task -> process_csv_task
    list_files >> read_csv_task >> process_csv_task