# Usa una imagen base de Python oficial
FROM python:3.10-slim-buster

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos de requisitos (puedes crear un requirements.txt específico para Streamlit si quieres,
# pero por ahora, solo instalaremos las necesarias directamente)
# Aquí podríamos crear un requirements.txt dentro de streamlit_app/
# Por simplicidad y para empezar, instalaremos directamente:
RUN pip install --no-cache-dir streamlit pymongo pandas plotly

# Expone el puerto que usará Streamlit
EXPOSE 8501

# Copia los archivos de tu aplicación Streamlit al contenedor
COPY . /app

# Comando para ejecutar la aplicación Streamlit
# El app.py es el archivo que se ejecutará
CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]