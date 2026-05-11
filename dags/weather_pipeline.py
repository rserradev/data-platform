from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pendulum  # Para timezone-aware start_date
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
# Las claves coinciden con los nombres de parámetros de boto3.client()
# para poder usar el unpacking ** más abajo
MINIO_CONN = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": os.getenv("MINIO_ROOT_USER"),
    "aws_secret_access_key": os.getenv("MINIO_ROOT_PASSWORD"),
}

# Bucket para datos de clima
BUCKET = "weather-bronze"

# URL base de la API Open-Meteo (endpoint archive = datos históricos consolidados)
BASE_URL = "https://api.open-meteo.com/v1/forecast"

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
    # Fecha de ayer en horario de Chile (T-1 ingestion pattern)
    santiago = ZoneInfo("America/Santiago")
    ayer = (datetime.now(santiago) - timedelta(days=1)).strftime("%Y-%m-%d")
    fecha_ingesta = datetime.now(santiago).strftime("%Y-%m-%d")
    print(f"Consultando clima para: {ayer}")
    print(f"Fecha de ingesta: {fecha_ingesta}")

    # Cliente S3 de boto3 apuntando a MinIO local.
    # El operador ** desempaqueta el diccionario en argumentos nombrados,
    # equivalente a escribir endpoint_url=..., aws_access_key_id=..., etc.
    s3 = boto3.client("s3", **MINIO_CONN)

    # Verificar si el bucket existe; si no, crearlo (lazy creation pattern).
    # head_bucket lanza excepción si el bucket no existe o no es accesible.
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' ya existe")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' creado")

    # 3. Guardar cada ciudad como archivo Parquet en MinIO, con ruta organizada por ciudad y fecha
    fecha = datetime.now(santiago).strftime("%Y-%m-%d")
    timestamp = int(datetime.now(santiago).timestamp())

    for city, coords in CITIES.items():
        print(f"Consultando clima para {city} ({coords['lat']}, {coords['lon']})")
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": "temperature_2m,precipitation",   # ← sin _sum
            "past_days": 1,
            "forecast_days": 1,
            "timezone": "America/Santiago",
        }
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Extraer solo el bloque "hourly" que tiene forma de columnas
        hourly = data["hourly"]
        n = len(hourly["time"])

        # Construir el dict columnar para PyArrow, agregando metadata de ciudad
        columns = {
            "ciudad": [city] * n,
            "latitud": [coords["lat"]] * n,
            "longitud": [coords["lon"]] * n,
            "fecha_ingesta": [fecha_ingesta] * n,
            "time": hourly["time"],
            "temperature_2m": hourly["temperature_2m"],
            "precipitation": hourly["precipitation"],   # ← sin _sum
        }

        table = pa.Table.from_pydict(columns)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Guardar en MinIO con ruta organizada por ciudad y fecha
        key = f"{city}/fecha={fecha_ingesta}/weather.parquet"
        s3.upload_fileobj(buffer, BUCKET, key)
        print(f"Clima de {city} guardado en MinIO como '{key}'")

def transform_to_silver():
    #Crear conexión a MinIO
    s3 = boto3.client("s3", **MINIO_CONN)

    fecha_santiago = ZoneInfo("America/Santiago")
    fecha_actual = datetime.now(fecha_santiago).strftime("%Y-%m-%d")

    # Listar objetos en el bucket para la fecha actual
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"fecha={fecha_actual}/")
    print(f"Archivos encontrados para fecha {fecha_actual}: {response.get('KeyCount', 0)}")

    # Conectar a PostgreSQL
    conn = psycopg2.connect(**DB_CONN)


with DAG(
    "weather_pipeline",
    # start_date con timezone explícita (manejo correcto del horario chileno)
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    schedule="0 0 * * *",  # Diariamente a las 00:00 hora Chile
    catchup=False,
    tags=["weather", "bronze"],
) as dag:
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )