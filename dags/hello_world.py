from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

with DAG(
    dag_id='hello_world',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='A simple Hello World DAG'
) as dag:
    task = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world
    )