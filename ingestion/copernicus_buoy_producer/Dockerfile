FROM python:3.11-slim
WORKDIR /app
COPY copernicus_buoy_producer.py ./
RUN pip install --no-cache-dir kafka-python requests
CMD ["python","copernicus_buoy_producer.py"]
