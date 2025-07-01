import time, json, pandas as pd, yaml, copernicusmarine as cm, os, socket
from confluent_kafka import Producer
def load_config():
    with open('/app/config.yml', 'r') as f: return yaml.safe_load(f)
def create_producer(config):
    return Producer({'bootstrap.servers': config['kafka']['brokers'], 'client.id': socket.gethostname()})
def main():
    config, producer = load_config(), create_producer(config)
    platform_type_filter = os.getenv('PLATFORM_TYPE')
    topic = config['kafka']['topics'][f"insitu_{platform_type_filter.lower()}"]
    dataset_id, poll_interval = config['cmems']['insitu']['dataset_id'], config['cmems']['insitu']['poll_interval_s']
    variables = ["PSAL", "TEMP", "SLEV", "FLU2", "CPHL"]
    cm.login(username=os.getenv("COPERNICUS_USERNAME"), password=os.getenv("COPERNICUS_PASSWORD"))
    print(f"🚀 Producer Copernicus In-Situ ({platform_type_filter}) avviato. Topic: {topic}...")
    while True:
        try:
            print(f"INFO: Fetch per {platform_type_filter}...")
            start_time = pd.to_datetime('now', utc=True) - pd.Timedelta(hours=2)
            ds = cm.read_dataframe(dataset_id=dataset_id, dataset_part="latest", variables=variables, start_datetime=start_time.isoformat())
            df_filtered = ds[ds['platform_type'] == platform_type_filter].copy()
            if df_filtered.empty: print(f"INFO: Nessun nuovo dato da {platform_type_filter}.")
            else:
                for _, row in df_filtered.iterrows():
                    payload = {"buoy_id": row['platform_code'],"timestamp": row['time'].isoformat(),"latitude": row['latitude'],"longitude": row['longitude']}
                    for var in variables:
                        if var in row and pd.notna(row[var]): payload[var] = row[var]
                    producer.produce(topic, key=payload['buoy_id'], value=json.dumps(payload, default=str))
                producer.flush()
                print(f"✅ Inviati {len(df_filtered)} record da {platform_type_filter}.")
        except Exception as e: print(f"🔥 Errore nel producer {platform_type_filter}: {e}")
        time.sleep(poll_interval)
if __name__ == "__main__": main()
