from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
import os

def fetch_weather():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    db = client["weather_db"]
    collection = db["forecast"]

    lat, lon = 40.7128, -74.0060  # Nueva York
    api_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    data = response.json()
    current_weather = data.get("current_weather", {})
    current_weather["fetched_at"] = datetime.utcnow()

    # inserta como documento independiente
    collection.insert_one(current_weather)
    print("Weather data inserted successfully.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("weather_forecast_dag", default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:
    task_fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather
    )
