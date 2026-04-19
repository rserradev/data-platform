from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
    "uf": "F073.UFF.PRE.Z.D",
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


# Funcion para traer indicadores en capa bronze
def fetch_indicators():

    santiago = ZoneInfo("America/Santiago")
    ahora = datetime.now(santiago)
    # Fecha de ayer
    ayer = (ahora - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Consultando indicadores para: {ayer}")

    # Definir la fecha de ingesta
    fecha_actual = ahora.strftime("%Y-%m-%d")
    print(f"Fecha de ingesta: {fecha_actual}")

    # Conectar a MinIO
    s3 = boto3.client("s3", **MINIO_CONN)

    # Crear bucket si no existe
    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' creado en MinIO.")
    except Exception:
        print(f"Bucket '{BUCKET}' ya existe en MinIO.")

    # Recorrer cada indicador y llamar a la API
    for nombre, serie_id in INDICATORS.items():
        params = {
            "user": BCENTRAL_USER,
            "pass": BCENTRAL_PASSWORD,
            "firstdate": ayer,
            "lastdate": ayer,
            "timeseries": serie_id,
            "function": "GetSeries",
        }

        response = requests.get(BASE_URL, params=params, timeout=30)
        data = response.json()
        print(f"Indicador {nombre} recibido: {len(data['Series']['Obs'])} registros")

        # Obtener las observaciones
        observaciones = data["Series"]["Obs"]

        # Convertir a tabla Parquet
        table = pa.table(
            {
                "indicador": [nombre] * len(observaciones),
                "serie_id": [serie_id] * len(observaciones),
                "fecha": [obs["indexDateString"] for obs in observaciones],
                "valor": [obs["value"] for obs in observaciones],
                "status": [obs["statusCode"] for obs in observaciones],
                "fetched_at": [ahora.isoformat()] * len(observaciones),
            }
        )

        # Escribir en memoria y subir a MinIO
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        key = f"indicators/{fecha_actual}/{nombre}_{ayer}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
        print(f"MinIO: {nombre} → {key}")


# Funcion para transformar datos a capa silver
def transform_to_silver():
    # Crear coneixón a MinIO
    s3 = boto3.client("s3", **MINIO_CONN)

    santiago = ZoneInfo("America/Santiago")
    fecha_actual = datetime.now(santiago).strftime("%Y-%m-%d")

    # Listar todos los archivos dentro del bucket que esten con el prefijo de la fecha actual
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"indicators/{fecha_actual}/")

    # Devuelve un diccionario con varios campos. Contents es la lista de archivos encontrados, si no hay archivos devuelve un diccionario vacío.
    archivos = response.get("Contents", [])
    print(f"Archivos encontrados en MinIO para transformación: {len(archivos)}")

    # Conectar a PostgreSQL
    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn.cursor() as cur:
            for obj in archivos:
                # Leer archivo Parquet desde MinIO
                file_obj = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                buffer = io.BytesIO(file_obj["Body"].read())
                table = pq.read_table(buffer)
                df = table.to_pydict()

                for i in range(len(df["fecha"])):
                    # Filtrar solo registros con datos válidos
                    if df["status"][i] != "OK":
                        continue

                    cur.execute(
                        """
                        INSERT INTO bcentral.silver_indicators (
                            indicador, serie_id, fecha,
                            valor, fetched_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (indicador, fecha) DO NOTHING
                    """,
                        (
                            df["indicador"][i],
                            df["serie_id"][i],
                            datetime.strptime(df["fecha"][i], "%d-%m-%Y").strftime(
                                "%Y-%m-%d"
                            ),
                            float(df["valor"][i]),
                            df["fetched_at"][i],
                        ),
                    )
                    print(
                        f"Silver: {df['indicador'][i]} {df['fecha'][i]} = {df['valor'][i]}"
                    )
        conn.commit()
        print("Transformación a silver completada")
    finally:
        conn.close()


with DAG(
    dag_id="bcentral_pipeline",
    description="Pipeline para extraer indicadores del Banco Central, almacenar en MinIO y cargar en PostgreSQL",
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bcentral", "indicators", "economia"],
) as dag:

    tarea_fetch = PythonOperator(
        task_id="fetch_indicators",
        python_callable=fetch_indicators,
    )

    tarea_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )

    tarea_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir profiles --select bcentral",
    )

    tarea_fetch >> tarea_silver >> tarea_dbt

if __name__ == "__main__":
    fetch_indicators()
    transform_to_silver()
