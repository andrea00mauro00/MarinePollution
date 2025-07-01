import os, time, json, requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC  = os.getenv('KAFKA_TOPIC')
API_URL      = os.getenv('COPERNICUS_API_URL')
API_USER     = os.getenv('COPERNICUS_API_USER')
API_PWD      = os.getenv('COPERNICUS_API_PASSWORD')
BUOY_ID      = os.getenv('COPERNICUS_BUOY_ID')
POLL_INTERVAL= int(os.getenv('POLL_INTERVAL_SECONDS',300))

# Geographic bounds for the request. Defaults target the Mediterranean area but
# can be overridden via environment variables.
LONGITUDE_MIN = float(os.getenv('COPERNICUS_LONGITUDE_MIN', '8.0'))
LONGITUDE_MAX = float(os.getenv('COPERNICUS_LONGITUDE_MAX', '9.0'))
LATITUDE_MIN  = float(os.getenv('COPERNICUS_LATITUDE_MIN', '44.0'))
LATITUDE_MAX  = float(os.getenv('COPERNICUS_LATITUDE_MAX', '45.0'))

def init_kafka():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except NoBrokersAvailable:
            print("⚠️ Kafka non pronto, riprovo tra 5s")
            time.sleep(5)

def fetch_buoy_data():
    payload = {
        'service_id': 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS',
        'product_id': 'global-analysis-forecast-phy-l4',
        'longitude_min': LONGITUDE_MIN,
        'longitude_max': LONGITUDE_MAX,
        'latitude_min': LATITUDE_MIN,
        'latitude_max': LATITUDE_MAX,
        'date_min':'NOW-PT5M','date_max':'NOW',
        'variable':['sea_surface_temperature','salinity'],
        'motu': API_URL, 'user': API_USER, 'password': API_PWD
    }
    r = requests.post(API_URL, data=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    producer = init_kafka()
    print("✅ Connesso a Kafka")
    while True:
        try:
            data = fetch_buoy_data()
            record = {
                'buoy_id': BUOY_ID,
                'timestamp': data.get('time'),
                'sst': data.get('sea_surface_temperature'),
                'salinity': data.get('salinity'),
            }
            producer.send(KAFKA_TOPIC, record)
            producer.flush()
            print(f"▶️ Inviati dati boa {BUOY_ID}")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("👋 Chiusura manuale.")
            break
        except Exception as e:
            print(f"❌ Errore: {e}")
            time.sleep(10)

if __name__ == '__main__':
    main()
