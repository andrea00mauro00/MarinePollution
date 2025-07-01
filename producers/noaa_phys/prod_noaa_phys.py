import time, json, requests, pandas as pd, yaml, socket
from confluent_kafka import Producer
def load_config():
    with open('/app/config.yml', 'r') as f: return yaml.safe_load(f)
def create_producer(config):
    return Producer({'bootstrap.servers': config['kafka']['brokers'], 'client.id': socket.gethostname()})
def main():
    config, producer, topic, url, poll_interval = load_config(), create_producer(config), config['kafka']['topics']['physical'], config['noaa']['url'], config['noaa']['poll_interval_s']
    print(f"🚀 Producer NOAA avviato. Topic: '{topic}'...")
    while True:
        try:
            r = requests.get(url); r.raise_for_status()
            lines = r.text.splitlines()
            data = [l.split() for l in lines[2:]]
            df = pd.DataFrame(data, columns=lines[0].replace("#", "").split())
            df.replace("MM", pd.NA, inplace=True)
            phys_vars = ['WTMP', 'ATMP', 'WDIR', 'WSPD', 'WVHT', 'DPD', 'CURD', 'CURS']
            for _, row in df.iterrows():
                try:
                    ts = pd.to_datetime(f"{row['YYYY']}-{row['MM']}-{row['DD']} {row['hh']}:{row['mm']}", errors='coerce')
                    if pd.isna(ts): continue
                    p = {"buoy_id": row['#STN'], "timestamp": ts.isoformat(), "latitude": pd.to_numeric(row.get('LAT'), errors='coerce'), "longitude": pd.to_numeric(row.get('LON'), errors='coerce')}
                    for var in phys_vars:
                        if var in row and pd.notna(row[var]): p[var] = pd.to_numeric(row[var], errors='coerce')
                    producer.produce(topic, key=p['buoy_id'], value=json.dumps({k: v for k, v in p.items() if pd.notna(v)}))
                except Exception: continue
            producer.flush()
            print(f"✅ Inviati {len(df)} record fisici dalla NOAA.")
        except Exception as e: print(f"🔥 Errore nel producer NOAA: {e}")
        time.sleep(poll_interval)
if __name__ == "__main__": main()
