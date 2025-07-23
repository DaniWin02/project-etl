# ETL Pipeline Project: Weather, Universities, and Books Data

**Author:** Daniel Gomez  
**Group:** B  
**Degree:** Ingeniería de Datos – Universidad Politécnica de Yucatán

## Project Overview

This project implements an end-to-end batch ETL (Extract, Transform, Load) pipeline using **Apache Airflow** for orchestration, **MongoDB** for data storage, and **Streamlit** for data visualization. The solution is fully containerized using Docker Compose.

The pipeline extracts data from three different public APIs, processes and enriches the data, and then visualizes it through a user-friendly dashboard.

## APIs Used

1.  **Open Library API**  
   `https://openlibrary.org/search.json?q=harry+potter`  
   → Retrieves metadata about Harry Potter books.

2.  **Universities API by Hipolabs**  
   `http://universities.hipolabs.com/search?country=Canada`  
   → Retrieves a list of universities in Canada.

3.  **Open-Meteo Weather API**  
   `https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true`  
   → Provides real-time weather data based on latitude and longitude.

## 🗃️ MongoDB vs PostgreSQL

- **PostgreSQL**: Used internally by Airflow to store metadata like DAG runs, task logs, and schedules.
- **MongoDB**: Used to store both **raw** and **processed** datasets from the APIs. It's the main database for storing and querying the ETL data.

## 🐳 How to Run the Project

1. Clone the repository:
   ```bash
   git clone https://github.com/DaniWin02/project-etl.git
   cd project-etl

2. Launch all services using Docker Compose:

bash
docker-compose up --build

This will launch:

Airflow Webserver at http://localhost:8080

Streamlit Dashboard at http://localhost:8501

MongoDB on port 27017
3. Access Airflow UI:

Go to http://localhost:8080

Trigger the DAG named main_pipeline manually.

You can monitor task execution and view logs from the UI.

4. Access Streamlit Dashboard:

After data is processed and loaded into MongoDB, open http://localhost:8501 to view the visualizations.

5. XCom Usage
Airflow's XCom (Cross-Communication) is used to pass data between tasks during runtime.
In this project, XComs are used to:

Share record counts between ingestion and transformation tasks.

Dynamically transfer extracted metadata for logging or filtering.

### Airflow DAG Screenshot
![Airflow DAG](/airflow_screenshot.png)

### Streamlit Dashboard Screenshot
![Streamlit Dashboard](/dashboard_screenshot.png)
