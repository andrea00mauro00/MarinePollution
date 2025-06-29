FROM python:3.11-slim
WORKDIR /app
COPY ingestion/water_metrics_loader/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia script, schema e config
COPY ingestion/water_metrics_loader/load_metrics.py ./
COPY ingestion/water_metrics_loader/water_metrics_schema.json ./
COPY ingestion/water_metrics_loader/config.yml ./config.yml

CMD ["python", "load_metrics.py"]
