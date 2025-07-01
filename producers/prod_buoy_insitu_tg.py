import time, json, pandas as pd, yaml, copernicusmarine as cm, socket
from confluent_kafka import Producer

def load_config():
    with open('config.yml', 'r') as f: return yaml.safe_load(f)

def create_producer(config):
    return Producer({'bootstrap.servers': config['kafka']['brokers'], 'client.id': socket.gethostname()})

def main():
    config, producer = load_config(), create_producer(config)
    dataset_id = config['cmems']['insitu']['dataset_id']
    poll_interval = config['cmems']['insitu']['poll_interval_s']
    topic = config['kafka']['topics']['insitu_tg']
    variables = ["PSAL", "TEMP", "SLEV", "FLU2", "CPHL"]
    print(f"🚀 Producer Copernicus (Tide Gauges) avviato. Polling ogni {poll_interval}s...")
    while True:
        try:
            print(f"INFO: Fetch per Tide Gauges (TG)...")
            start_time = pd.to_datetime('now', utc=True) - pd.Timedelta(hours=2)
            ds = cm.read_dataframe(dataset_id=dataset_id, dataset_part="latest", variables=variables, start_datetime=start_time.isoformat())
            df_tg = ds[ds['platform_type'] == 'TG'].copy()
            if df_tg.empty: print("INFO: Nessun nuovo dato da Tide Gauges.")
            else:
                for _, row in df_tg.iterrows():
                    payload = {"buoy_id": row['platform_code'], "timestamp": row['time'].isoformat(), "latitude": row['latitude'], "longitude": row['longitude']}
                    for var in variables:
                        if var in row and pd.notna(row[var]): payload[var] = row[var]
                    producer.produce(topic, key=payload['buoy_id'], value=json.dumps(payload))
                producer.flush()
                print(f"✅ Inviati {len(df_tg)} record da Tide Gauges.")
        except Exception as e: print(f"🔥 Errore nel producer Tide Gauges: {e}")
        time.sleep(poll_interval)

if __name__ == "__main__":
    cm.login()
    main()
