from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
import os

def fetch_universities():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    db = client["universities_db"]
    collection = db["universities"]

    api_url = "http://universities.hipolabs.com/search?country=Canada"

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    raw_data = response.json()

    inserted_count = 0

    for uni in raw_data:
        web_pages = uni.get("web_pages", [])
        # aseguramos que sea una lista con al menos un elemento
        web_page = web_pages[0] if isinstance(web_pages, list) and web_pages else ""

        doc = {
            "name": uni.get("name", ""),
            "country": uni.get("country", ""),
            "web_page": web_page,
            "alpha_two_code": uni.get("alpha_two_code", ""),
            "fetched_at": datetime.utcnow()
        }
        collection.insert_one(doc)
        inserted_count += 1

    print(f"Inserted {inserted_count} universities successfully.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("universities_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    task_fetch_universities = PythonOperator(
        task_id="fetch_universities",
        python_callable=fetch_universities
    )
