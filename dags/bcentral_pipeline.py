from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import json

# Credenciales desde variables de entorno
BCENTRAL_USER = os.getenv("BCENTRAL_USER")
BCENTRAL_PASSWORD = os.getenv("BCENTRAL_PASSWORD")

# Indicadores a consultar
INDICATORS = {
    "usd_clp": "F073.TCO.PRE.Z.D",
    "uf":      "F073.UFF.PRE.Z.D",
}

# Conexión a la base de datos
DB_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# Configuración de MinIO
MINIO_CONN = {
    "endpoint_url": "http://minio:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
}

# Bucket para almacenar los datos en MinIO
BUCKET = "bcentral-bronze"

# URL base de la API del Banco Central
BASE_URL = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"

def fetch_indicators():
    # Fecha de ayer
    ayer = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Consultando indicadores para: {ayer}")
    
    # Conectar a MinIO
    s3 = boto3.client("s3", **MINIO_CONN)
    fecha = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"Fecha de ingesta: {fecha}")

    # Recorrer cada indicador y llamar a la API
    for nombre, serie_id in INDICATORS.items():
        params = {
            "user": BCENTRAL_USER,
            "pass": BCENTRAL_PASSWORD,
            "firsdate": ayer,
            "lastdate": ayer,
            "timeseries": serie_id,
            "function": "GetSeries",
        }
        response = requests.get(BASE_URL, params=params, timeout=30)
        data = response.json()
        print(data)  # Imprimir la respuesta completa para depuración
        print(f"Indicador {nombre} recibido: {len(data.get('Series', []))} registros")

if __name__ == "__main__":
    fetch_indicators()