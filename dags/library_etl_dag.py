from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
import os

def fetch_books():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    db = client["books_db"]
    collection = db["books"]

    api_url = "https://openlibrary.org/search.json?q=harry+potter"

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    raw_data = response.json()
    docs = raw_data.get("docs", [])

    for doc in docs:
        book = {
            "title": doc.get("title", ""),
            "author": doc.get("author_name", [""])[0] if doc.get("author_name") else "",
            "first_publish_year": doc.get("first_publish_year", None),
            "isbn": doc.get("isbn", [""])[0] if doc.get("isbn") else "",
            "fetched_at": datetime.utcnow()
        }
        collection.insert_one(book)

    print(f"Inserted {len(docs)} books successfully.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("open_library_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    task_fetch_books = PythonOperator(
        task_id="fetch_books",
        python_callable=fetch_books
    )
