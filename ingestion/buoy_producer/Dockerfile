FROM python:3.11-slim
WORKDIR /app

# Copia e installa dipendenze Python
COPY ingestion/buoy_producer/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia schema e script dal percorso corretto
COPY ingestion/buoy_producer/buoy_schema.json ./buoy_schema.json
COPY ingestion/buoy_producer/prod_buoy.py ./prod_buoy.py

# Copia config condiviso
COPY ingestion/common/config.yml ./config.yml

# Comando di avvio
CMD ["python", "prod_buoy.py"]
