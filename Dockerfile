# Usa la imagen base de Airflow especificada
FROM apache/airflow:2.8.1-python3.10

# Copia el archivo de requisitos
# Lo copiamos en un directorio temporal accesible para el usuario Airflow
COPY requirements.txt /opt/airflow/requirements.txt

# Cambia al usuario 'airflow' antes de instalar.
# El directorio /opt/airflow y sus subdirectorios son escribibles por el usuario airflow.
USER airflow

# Instala las dependencias.
# Se asegura de que pip se ejecute con el usuario 'airflow'
# y que tenga permisos para escribir en el entorno virtual predeterminado de Airflow.
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Establece el directorio de trabajo donde Airflow buscará los DAGs
WORKDIR /opt/airflow