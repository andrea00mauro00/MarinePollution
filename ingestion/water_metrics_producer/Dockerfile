FROM python:3.11-slim
WORKDIR /app
COPY water_metrics_producer.py ./
RUN pip install --no-cache-dir kafka-python requests
CMD ["python","water_metrics_producer.py"]
