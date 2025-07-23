import os
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
client = MongoClient(MONGO_URI)
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://airflow-webserver:8080/api/v1")
AIRFLOW_UI_URL = os.getenv("AIRFLOW_UI_URL", "http://airflow-webserver:8080")

airflow_user = os.getenv("AIRFLOW_USER", "admin")
airflow_pass = os.getenv("AIRFLOW_PASSWORD", "admin")

def test_airflow_connection():
    try:
        url = f"{AIRFLOW_API_BASE}/version"
        response = requests.get(url, auth=HTTPBasicAuth(airflow_user, airflow_pass), timeout=10)
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        return None, str(e)

# Fetch functions
def fetch_data(db_name, col_name, limit):
    db = client[db_name]
    col = db[col_name]
    return list(col.find().sort("fetched_at", -1).limit(limit))

def get_dag_runs(dag_id, limit=5):
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
    params = {'order_by': '-execution_date', 'limit': limit}
    response = requests.get(url, params=params, auth=HTTPBasicAuth(airflow_user, airflow_pass), timeout=30)
    if response.status_code == 200:
        return response.json().get("dag_runs", [])
    else:
        st.error(f"Error fetching DAG runs: {response.status_code}")
        return []

def main():
    st.set_page_config(page_title="ETL Dashboard", layout="wide")
    st.title("ETL Monitoring Dashboard")

    # Sidebar
    with st.sidebar:
        st.header("Settings")
        if st.button("Test Airflow Connection"):
            status_code, response_text = test_airflow_connection()
            if status_code == 200:
                st.success(f"Connected to Airflow (Status: {status_code})")
                st.json(json.loads(response_text))
            else:
                st.error(f"Connection failed (Status: {status_code})")

        st.markdown("---")
        st.header("Filters")
        limit = st.slider("Number of records to display:", 5, 100, 10, step=5)

    # Layout
    st.markdown("## Latest ETL Data")

    col1, col2, col3 = st.columns(3)

    # Weather
    with col1:
        weather_docs = fetch_data("weather_db", "forecast", limit)
        if weather_docs:
            df_weather = pd.json_normalize(weather_docs).drop(columns=["_id"], errors="ignore")
            st.markdown("### Weather Forecast")
            st.dataframe(df_weather, use_container_width=True)

            if "temperature" in df_weather.columns and "fetched_at" in df_weather.columns:
                df_weather["fetched_at"] = pd.to_datetime(df_weather["fetched_at"])
                df_weather = df_weather.sort_values("fetched_at")

                # Preparamos dataframe con fecha y temperatura promedio
                df_temp = df_weather.set_index("fetched_at")[["temperature"]]

                st.line_chart(df_temp, use_container_width=True)

                st.markdown("Temperatura promedio a lo largo del tiempo")
            else:
                st.info("No hay datos de temperatura suficientes para graficar.")
        else:
            st.info("No weather data available.")

    # Universities
    with col2:
        uni_docs = fetch_data("universities_db", "universities", limit)
        if uni_docs:
            df_uni = pd.json_normalize(uni_docs).drop(columns=["_id"], errors="ignore")
            st.markdown("### Universities")

            selected_country = st.selectbox(
                "Filter by Country:",
                options=["All"] + sorted(df_uni["country"].dropna().unique().tolist())
            )
            if selected_country != "All":
                df_uni = df_uni[df_uni["country"] == selected_country]

            st.dataframe(df_uni, use_container_width=True)

            if "name" in df_uni.columns:
                name_counts = df_uni["name"].value_counts().head(20)
                st.bar_chart(name_counts)
            else:
                st.info("No hay datos de nombre de universidades para graficar.")
        else:
            st.info("No university data available.")

    # Books
    with col3:
        book_docs = fetch_data("books_db", "books", limit)
        if book_docs:
            df_books = pd.json_normalize(book_docs).drop(columns=["_id"], errors="ignore")
            st.markdown("### Books")
            selected_author = st.selectbox(
                "Filter by Author:",
                options=["All"] + sorted(df_books["author"].dropna().unique().tolist())
            )
            if selected_author != "All":
                df_books = df_books[df_books["author"] == selected_author]
            st.dataframe(df_books, use_container_width=True)

            if "first_publish_year" in df_books.columns:
                year_counts = df_books["first_publish_year"].value_counts().sort_index()
                st.line_chart(year_counts)
        else:
            st.info("No book data available.")

    # Airflow DAG Runs
    st.markdown("---")
    st.markdown("## Airflow DAG Runs")

    dag_list = ["weather_forecast_dag", "universities_dag", "open_library_dag"]
    selected_dag = st.selectbox("Select a DAG", dag_list)

    if st.button("Refresh DAG Runs"):
        runs = get_dag_runs(selected_dag)
        if runs:
            df_runs = pd.DataFrame(runs)
            st.dataframe(df_runs, use_container_width=True)
            st.markdown(f"[Open Airflow UI]({AIRFLOW_UI_URL}/dags/{selected_dag}/grid)")
        else:
            st.info("No DAG runs found or unable to fetch.")

if __name__ == "__main__":
    main()
