import time
import json
import sys
import requests
import uuid
from datetime import datetime
from kafka import KafkaProducer
import random

# Configurazione
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'satellite_imagery_raw'

# Intervallo di polling in secondi (ogni 10 minuti)
POLL_INTERVAL_SECONDS = 600

def create_kafka_producer():
    """Crea e restituisce un producer Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connessione a Kafka stabilita con successo!")
        return producer
    except Exception as e:
        print(f"❌ Errore di connessione a Kafka: {e}")
        sys.exit(1)

def generate_satellite_data():
    """
    Genera dati simulati di immagini satellitari.
    In un ambiente reale, questi dati verrebbero scaricati da API satellitari.
    """
    # Genera coordinate casuali per diverse aree marine
    areas = [
        {"name": "North Atlantic", "lat_range": (30, 45), "lon_range": (-75, -50)},
        {"name": "Mediterranean", "lat_range": (35, 45), "lon_range": (5, 25)},
        {"name": "Gulf of Mexico", "lat_range": (20, 30), "lon_range": (-98, -80)},
        {"name": "South Pacific", "lat_range": (-30, -10), "lon_range": (150, 180)}
    ]
    
    area = random.choice(areas)
    lat = random.uniform(*area["lat_range"])
    lon = random.uniform(*area["lon_range"])
    
    # Genera dati simulati di inquinamento
    pollution_types = ["oil_spill", "algal_bloom", "plastic_concentration", "chemical_discharge"]
    pollution_type = random.choice(pollution_types)
    
    # Simula un'immagine satellitare (in realtà sarebbe un URL o un ID di un'immagine)
    image_id = str(uuid.uuid4())
    
    # Prepara i metadati
    metadata = {
        'image_id': image_id,
        'timestamp': datetime.now().isoformat(),
        'lat': lat,
        'lon': lon,
        'area_name': area["name"],
        'pollution_type': pollution_type,
        'severity': random.choice(["low", "medium", "high"]),
        'detection_confidence': round(random.uniform(0.7, 0.99), 2),
        'source': 'satellite_simulation'
    }
    
    return metadata

def main():
    """Ciclo principale del producer."""
    producer = create_kafka_producer()
    
    print(f"🚀 Producer satellitare avviato. Invio dati al topic '{KAFKA_TOPIC}'.")
    
    while True:
        try:
            satellite_data = generate_satellite_data()
            
            print(f"📡 Dati satellitari generati: {satellite_data['image_id']} - {satellite_data['pollution_type']} in {satellite_data['area_name']}")
            producer.send(KAFKA_TOPIC, value=satellite_data)
            producer.flush()
            print(f"✅ Metadati dell'immagine inviati con successo a Kafka.")
            
            print(f"⏳ Prossimo controllo tra {POLL_INTERVAL_SECONDS // 60} minuti...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            print("\n👋 Rilevato arresto manuale. Arrivederci!")
            break
        except Exception as e:
            print(f"❌ Errore inaspettato nel ciclo principale: {e}")
            time.sleep(60)

if __name__ == '__main__':
    main()