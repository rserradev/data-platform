from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
import psycopg2
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import os

# Conexión a PostgreSQL
DB_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# Configuración de MinIO
MINIO_CONN = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": os.getenv("MINIO_ROOT_USER"),
    "aws_secret_access_key": os.getenv("MINIO_ROOT_PASSWORD"),
}

# Bucket para datos de clima
BUCKET = "weather-bronze"

# URL base de la API Open-Meteo
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Ciudades a monitorear
CITIES = {
    "santiago":    {"lat": -33.45, "lon": -70.66},
    "valparaiso":  {"lat": -33.04, "lon": -71.63},
    "concepcion":  {"lat": -36.82, "lon": -73.04},
    "antofagasta": {"lat": -23.65, "lon": -70.40},
    "la_serena":   {"lat": -29.90, "lon": -71.25},
    "temuco":      {"lat": -38.73, "lon": -72.59},
}

def fetch_weather():
       santiago = ZoneInfo("America/Santiago")
       ayer = (datetime.now(santiago) - timedelta(days=1)).strftime("%Y-%m-%d")
       fecha_ingesta = datetime.now(santiago).strftime("%Y-%m-%d")
       print(f"Consultando clima para: {ayer}")
       print(f"Fecha de ingesta: {fecha_ingesta}")

with DAG(
    "weather_pipeline",
    start_date=datetime(2024, 6, 1),
    schedule_interval="0 0 * * *",  # Ejecutar diariamente a medianoche
    catchup=False,
) as dag:
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )