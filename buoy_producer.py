import time
import json
import requests
import random
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'buoy_raw'
# --- NUOVA BOA REALE NELL'ATLANTICO ---
# Stazione 13002: Prediction and Research Moored Array
BUOY_ID = '13002'
NOAA_URL = f'https://www.ndbc.noaa.gov/data/realtime2/{BUOY_ID}.txt'
POLL_INTERVAL_SECONDS = 60 * 5 # Intervallo: 5 minuti

def create_kafka_producer():
    """Crea e restituisce un producer Kafka."""
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connessione a Kafka stabilita con successo!")
            return producer
        except NoBrokersAvailable:
            print(f"🔥 Impossibile connettersi a Kafka. Tentativo {i+1} di {retries}...")
            time.sleep(5)
    print("❌ Connessione a Kafka fallita dopo diversi tentativi.")
    sys.exit(1)

def fetch_buoy_data():
    """Scarica e processa i dati della boa dalla NOAA."""
    print(f"📡 Chiamata alla boa NOAA nell'Atlantico: {NOAA_URL}")
    try:
        headers = {'User-Agent': 'MarinePollutionProject/1.0'}
        response = requests.get(NOAA_URL, timeout=10, headers=headers)
        response.raise_for_status()

        lines = response.text.splitlines()
        if len(lines) < 3:
            return None

        header = lines[0].replace("#", "").split()
        latest_data = lines[2].split()

        data_dict = dict(zip(header, latest_data))

        processed_data = {}
        for key, value in data_dict.items():
            if value != 'MM':
                try:
                    processed_data[key] = float(value)
                except ValueError:
                    processed_data[key] = value

        return processed_data

    except requests.RequestException as e:
        print(f"Errore durante la richiesta HTTP: {e}")
        return None

def simulate_water_quality_metrics(data: dict):
    """Aggiunge metriche simulate di pH, torbidità e contaminanti ai dati reali."""
    if random.random() < 0.1:
        data['pH'] = random.choice([5.5, 9.0])
    else:
        data['pH'] = round(random.uniform(7.8, 8.4), 2)

    if random.random() < 0.1:
        data['turbidity'] = round(random.uniform(6.0, 10.0), 2)
    else:
        data['turbidity'] = round(random.uniform(0.2, 1.5), 2)

    if random.random() < 0.1:
        data['contaminant_ppm'] = round(random.uniform(8.0, 15.0), 2)
    else:
        data['contaminant_ppm'] = round(random.uniform(0.5, 2.0), 2)

    return data

def main():
    """Ciclo principale del producer."""
    producer = create_kafka_producer()

    print(f"🚀 Producer avviato. Invio dati dalla boa dell'Atlantico {BUOY_ID} al topic '{KAFKA_TOPIC}'.")

    while True:
        try:
            buoy_data = fetch_buoy_data()

            if buoy_data:
                buoy_data['sensor_id'] = BUOY_ID

                final_data = simulate_water_quality_metrics(buoy_data)

                print(f"📦 Dati preparati (Reali + Simulati): {json.dumps(final_data)}")

                producer.send(KAFKA_TOPIC, value=final_data)
                producer.flush()
                print(f"✅ Dati inviati con successo a Kafka.")

            else:
                print("⚠️ Nessun dato valido ricevuto dalla boa.")

            print(f"⏳ Prossimo controllo tra {POLL_INTERVAL_SECONDS / 60} minuti...")
            time.sleep(POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n👋 Rilevato arresto manuale. Arrivederci!")
            break
        except Exception as e:
            print(f"❌ Errore inaspettato nel ciclo principale: {e}")
            time.sleep(10)

if __name__ == '__main__':
    main()