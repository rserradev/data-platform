from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import psycopg2
import json
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io

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

BUCKET = "crypto-bronze"
COINS = ["bitcoin", "ethereum", "solana", "cardano"]

def fetch_prices():

    # 1. Llamar a la API
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
    }
    response = requests.get(url, params=params, timeout=30)
    data = response.json()
    print(f"Monedas recibidas: {len(data)}")

    # 2. Crear cliente de MinIO
    s3 = boto3.client("s3", **MINIO_CONN)

    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' creado en MinIO")
    except Exception:
        print(f"Bucket '{BUCKET}' ya existe en MinIO")

    # 3. Guardar cada moneda como archivo Parquet en MinIO
    fecha = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = int(datetime.utcnow().timestamp())

    for coin in data:
        # Convertir a tabla Parquet
        table = pa.table({
            "coin_id":       [coin["id"]],
            "symbol":        [coin["symbol"]],
            "name":          [coin["name"]],
            "price_usd":     [float(coin["current_price"])],
            "market_cap":    [float(coin["market_cap"] or 0)],
            "volume_24h":    [float(coin["total_volume"] or 0)],
            "change_24h":    [float(coin["price_change_percentage_24h"] or 0)],
            "fetched_at":    [datetime.utcnow().isoformat()],
            "raw_json":      [json.dumps(coin)],
        })

        # Escribir Parquet en memoria
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Subir a MinIO
        key = f"prices/{fecha}/{coin['id']}_{timestamp}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
        print(f"MinIO: {coin['name']} → {key}")


def transform_to_silver():
    # Conectar a MinIO y leer los archivos de hoy
    s3 = boto3.client("s3", **MINIO_CONN)
    fecha = datetime.utcnow().strftime("%Y-%m-%d")

    # Listar archivos de hoy
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"prices/{fecha}/")
    archivos = response.get("Contents", [])
    print(f"Archivos encontrados en MinIO: {len(archivos)}")

    # Leer el archivo más reciente de cada moneda
    ultimos = {}
    for obj in archivos:
        key = obj["Key"]
        coin_id = key.split("/")[-1].split("_")[0]
        if coin_id not in ultimos or key > ultimos[coin_id]:
            ultimos[coin_id] = key

    # Insertar en silver
    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn.cursor() as cur:
            for coin_id, key in ultimos.items():
                # Descargar archivo Parquet
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                buffer = io.BytesIO(obj["Body"].read())
                table = pq.read_table(buffer)
                row = table.to_pydict()

                cur.execute("""
                    INSERT INTO silver.prices (
                        coin_id, symbol, name, price_usd,
                        market_cap_usd, volume_24h_usd,
                        price_change_24h, fetched_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["coin_id"][0],
                    row["symbol"][0],
                    row["name"][0],
                    row["price_usd"][0],
                    row["market_cap"][0],
                    row["volume_24h"][0],
                    row["change_24h"][0],
                    row["fetched_at"][0],
                ))
                print(f"Silver: {row['name'][0]} - ${row['price_usd'][0]}")
        conn.commit()
        print("Transformación a silver completada")
    finally:
        conn.close()


with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    tarea_fetch = PythonOperator(
        task_id="fetch_prices",
        python_callable=fetch_prices,
    )

    tarea_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )

    tarea_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir profiles --select gold",
    )

    tarea_fetch >> tarea_silver >> tarea_dbt