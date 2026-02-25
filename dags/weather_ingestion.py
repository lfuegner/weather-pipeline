import os
import json
from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.client import Config

BERLIN_LAT = 52.5200
BERLIN_LON = 13.4050
BUCKET_NAME = "weather-raw"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

def get_minio_client():
    return boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="eu-central-1",
        )

def ensure_bucket_exists(client):
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if BUCKET_NAME not in existing:
        client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created Bucket: {BUCKET_NAME}")


def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
            "latitude": BERLIN_LAT,
            "longitude": BERLIN_LON,
            "hourly": [
                "temperature_2m",
                "precipitation",
                "windspeed_10m",
                "relativehumidity_2m",
            ],
        "timezone": "Europe/Berlin",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data =response.json()

    client = get_minio_client()
    ensure_bucket_exists(client)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"berlin/weather_{timestamp}.json"

    client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )

    print(f"Uploaded to MinIO: s3://{BUCKET_NAME}/{key}")
    return key

with DAG(
    dag_id = "weather_ingestion",
    start_date = datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["weather", "ingestion"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_berlin_weather",
        python_callable=fetch_weather,
    )
