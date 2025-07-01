FROM python:3.11-slim
WORKDIR /app
COPY producers/noaa_phys/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/config.yml /app/
COPY producers/noaa_phys/prod_noaa_phys.py .
CMD ["python", "prod_noaa_phys.py"]
