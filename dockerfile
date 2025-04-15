FROM apache/airflow:2.10.5

USER airflow
RUN pip install requests duckdb apache-airflow-providers-slack==8.9.0