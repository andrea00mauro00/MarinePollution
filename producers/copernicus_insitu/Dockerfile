FROM python:3.11-slim
WORKDIR /app
COPY producers/copernicus_insitu/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/config.yml /app/
COPY producers/copernicus_insitu/prod_copernicus_insitu.py .
CMD ["python", "prod_copernicus_insitu.py"]
