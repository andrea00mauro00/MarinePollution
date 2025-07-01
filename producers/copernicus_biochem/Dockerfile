FROM python:3.11-slim
WORKDIR /app
COPY producers/copernicus_biochem/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/config.yml /app/
COPY producers/copernicus_biochem/prod_copernicus_biochem.py .
CMD ["python", "prod_copernicus_biochem.py"]
