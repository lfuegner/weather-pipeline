import os
import json
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BERLIN_LAT = 52.5200
BERLIN_LOT = 13.4050
OUTPUT_DIR = "/opt/airflow/data/raw"

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
            "latitude": BERLIN_LAT,
            "longitude": BERLIN_LOT,
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

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{OUTPUT_DIR}/berline_weather_{timestamp}.json"

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved weather data to {filename}")
    return filename

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
