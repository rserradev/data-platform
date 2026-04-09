FROM apache/airflow:2.9.1

# Instalar dbt-postgres como el usuario airflow
RUN pip install dbt-postgres==1.7.0 boto3 pyarrow