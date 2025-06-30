import time
import json
import requests
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configurazione ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'water_metrics_raw'
POLL_INTERVAL_SECONDS = 60 * 5 # Intervallo: 5 minuti

# --- NUOVA Configurazione API USGS ---
# Stazione 09380000: Colorado River at Lees Ferry, AZ (molto affidabile)
# Parametri: 00400 = pH, 63680 = Torbidità (FNU)
SITE_ID = '09380000'
PARAMETER_CODES = '00400,63680'
USGS_API_URL = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={SITE_ID}&parameterCd={PARAMETER_CODES}"

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

def fetch_usgs_data():
    """Scarica e processa i dati di qualità dell'acqua dall'API USGS."""
    print(f"📡 Chiamata all'API USGS per la stazione {SITE_ID}...")
    try:
        headers = {'User-Agent': 'MarinePollutionProject/1.0'}
        response = requests.get(USGS_API_URL, timeout=20, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        time_series = data['value']['timeSeries']
        if not time_series:
            print(f"⚠️ L'API USGS non ha restituito dati per la stazione {SITE_ID}.")
            return None

        latest_metrics = {'sensor_id': SITE_ID}
        
        for ts in time_series:
            param_code = ts['variable']['variableCode'][0]['value']
            
            if ts['values'] and ts['values'][0]['value']:
                last_value = ts['values'][0]['value'][0]
                value = float(last_value['value'])
                timestamp = last_value['dateTime']
                
                if param_code == '00400': # pH
                    latest_metrics['pH'] = value
                elif param_code == '63680': # Torbidità
                    latest_metrics['turbidity'] = value
                
                latest_metrics['timestamp'] = timestamp

        if 'pH' not in latest_metrics and 'turbidity' not in latest_metrics:
            print("⚠️ Nessuna metrica di pH o torbidità trovata nei dati ricevuti.")
            return None
            
        return latest_metrics

    except requests.RequestException as e:
        print(f"❌ Errore durante la richiesta HTTP: {e}")
        return None
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        print(f"❌ Errore durante il parsing della risposta JSON: {e}")
        return None

def main():
    """Ciclo principale del producer."""
    producer = create_kafka_producer()
    
    print(f"🚀 Producer avviato. Invio dati REALI dal sito USGS {SITE_ID} al topic '{KAFKA_TOPIC}'.")
    
    while True:
        try:
            water_data = fetch_usgs_data()
            
            if water_data:
                print(f"💧 Dati reali ricevuti: {json.dumps(water_data)}")
                producer.send(KAFKA_TOPIC, value=water_data)
                producer.flush()
                print(f"✅ Dati inviati con successo a Kafka.")
            else:
                print("⚠️ Nessun dato valido ricevuto da USGS in questo ciclo.")

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