import json
import time
import sys
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from shapely.geometry import Point

# Configurazione
KAFKA_TOPICS = ['buoy_raw', 'water_metrics_raw', 'sensor_alerts']
KAFKA_BROKER = 'localhost:9092'
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marinets',
    'user': 'marine',
    'password': 'marinepw'
}

def create_db_connection():
    """Crea e restituisce una connessione al database."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Errore di connessione a PostgreSQL: {e}")
        sys.exit(1)

def get_location(data):
    """Estrae o genera la posizione da un record."""
    # Coordinate di esempio per diverse boe/sensori
    sensor_locations = {
        '13002': (-68.502, 42.345),  # Boa NOAA Atlantico
        '09380000': (-111.588, 36.865),  # USGS Colorado River
    }
    
    sensor_id = data.get('sensor_id', 'unknown')
    
    if sensor_id in sensor_locations:
        lon, lat = sensor_locations[sensor_id]
        return Point(lon, lat)
    
    # Se non troviamo coordinate predefinite, cerchiamo nel record
    if 'lon' in data and 'lat' in data:
        return Point(data['lon'], data['lat'])
    
    # Default: coordinate null
    return None

def process_raw_data(topic, data, conn):
    """Processa dati grezzi da sensori."""
    sensor_id = data.get('sensor_id', 'unknown')
    location = get_location(data)
    timestamp = data.get('timestamp', datetime.now().isoformat())
    
    # Estrai coordinate
    lat, lon = None, None
    if location:
        lon, lat = location.x, location.y
    
    # Converti tutte le metriche numeriche in record singoli
    metrics = []
    for key, value in data.items():
        if isinstance(value, (int, float)) and key not in ['timestamp', 'lat', 'lon']:
            metrics.append((key, value))
    
    if metrics:
        with conn.cursor() as cursor:
            for metric, value in metrics:
                cursor.execute(
                    """INSERT INTO sensor_data 
                       (time, sensor_id, lat, lon, metric, value, source) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (timestamp, sensor_id, lat, lon, metric, value, topic)
                )
        print(f"✅ Salvati {len(metrics)} parametri da {sensor_id} ({topic})")

def process_anomaly(data, conn):
    """Processa dati di anomalie."""
    if not isinstance(data, dict):
        return
        
    sensor_id = data.get('sensor_id', 'unknown')
    location = get_location(data)
    timestamp = data.get('timestamp', datetime.now().isoformat())
    
    # Estrai coordinate
    lat, lon = None, None
    if location:
        lon, lat = location.x, location.y
    
    # Determina quale metrica ha causato l'anomalia
    if 'pH' in data and (data['pH'] < 6.5 or data['pH'] > 8.5):
        metric, value = 'pH', data['pH']
        severity = 2 if data['pH'] < 6.0 or data['pH'] > 9.0 else 1
    elif 'turbidity' in data and data['turbidity'] > 5:
        metric, value = 'turbidity', data['turbidity']
        severity = 2 if data['turbidity'] > 8 else 1
    elif 'contaminant_ppm' in data and data['contaminant_ppm'] > 7:
        metric, value = 'contaminant_ppm', data['contaminant_ppm']
        severity = 2 if data['contaminant_ppm'] > 10 else 1
    else:
        return
    
    with conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO anomalies 
               (time, sensor_id, lat, lon, metric, value, severity) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (timestamp, sensor_id, lat, lon, metric, value, severity)
        )
    print(f"⚠️ Anomalia salvata: {sensor_id} - {metric} = {value} (severità: {severity})")

def main():
    """Funzione principale del consumer."""
    print(f"🚀 Avvio del consumer PostgreSQL per i topic: {', '.join(KAFKA_TOPICS)}")
    
    # Crea connessione al database
    conn = create_db_connection()
    
    # Crea consumer Kafka
    try:
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='postgres-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"✅ Connesso a Kafka, in ascolto sui topic")
    except Exception as e:
        print(f"❌ Errore di connessione a Kafka: {e}")
        conn.close()
        sys.exit(1)
    
    # Loop principale
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            if topic == 'sensor_alerts':
                process_anomaly(data, conn)
            else:  # Dati raw
                process_raw_data(topic, data, conn)
                
    except KeyboardInterrupt:
        print("\n👋 Rilevato arresto manuale. Chiusura connessioni...")
    except Exception as e:
        print(f"❌ Errore inaspettato: {e}")
    finally:
        consumer.close()
        conn.close()
        print("✅ Connessioni chiuse. Arrivederci!")

if __name__ == '__main__':
    main()