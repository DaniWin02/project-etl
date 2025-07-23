from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# --- Función Placeholder ---
# Esta función es un simple 'noop' (no operation) que no hace nada.
# Solo está aquí para que el PythonOperator tenga algo que ejecutar.
def do_nothing():
    print("¡El DAG main_etl_pipeline está activo y funcionando!")

with DAG(
    dag_id='main_etl_pipeline',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'main', 'multi_api'],
    doc_md="""
    ### Main ETL Pipeline (Placeholder)
    Este DAG es un marcador de posición para verificar la visibilidad en la UI de Airflow.
    No realiza ninguna operación ETL real.
    """,
) as dag:

    # --- Tarea de Prueba ---
    # Una tarea simple que ejecuta la función 'do_nothing'
    # Solo para asegurar que el DAG tenga al menos una tarea válida.
    dummy_task = PythonOperator(
        task_id='check_pipeline_visibility',
        python_callable=do_nothing,
    )

    # Definición de un flujo simple (aunque sea una sola tarea)
    # Esto asegura que Airflow reconozca la tarea y el DAG como válidos.
    dummy_task