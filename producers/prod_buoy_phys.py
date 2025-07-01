import time, json, requests, pandas as pd, yaml, socket
from confluent_kafka import Producer

def load_config():
    with open('config.yml', 'r') as f: return yaml.safe_load(f)

def create_producer(config):
    return Producer({'bootstrap.servers': config['kafka']['brokers'], 'client.id': socket.gethostname()})

def fetch_noaa_data(url):
    response = requests.get(url)
    response.raise_for_status()
    lines = response.text.splitlines()
    data = [l.split() for l in lines[2:]]
    columns = lines[0].replace("#", "").split()
    df = pd.DataFrame(data, columns=columns)
    df.replace("MM", pd.NA, inplace=True)
    return df

def main():
    config = load_config()
    producer = create_producer(config)
    topic = config['kafka']['topics']['physical']
    url = config['ndbc_url']
    print(f"🚀 Producer NOAA avviato. Topic: '{topic}'...")
    while True:
        try:
            df = fetch_noaa_data(url)
            phys_vars = ['WTMP', 'ATMP', 'WDIR', 'WSPD', 'WVHT', 'DPD', 'CURD', 'CURS']
            for _, row in df.iterrows():
                try:
                    ts = pd.to_datetime(f"{row['YYYY']}-{row['MM']}-{row['DD']} {row['hh']}:{row['mm']}", errors='coerce')
                    if pd.isna(ts): continue
                    payload = {
                        "buoy_id": row['#STN'], "timestamp": ts.isoformat(),
                        "latitude": pd.to_numeric(row.get('LAT'), errors='coerce'),
                        "longitude": pd.to_numeric(row.get('LON'), errors='coerce'),
                    }
                    for var in phys_vars:
                        if var in row and pd.notna(row[var]): payload[var] = pd.to_numeric(row[var], errors='coerce')
                    producer.produce(topic, key=payload['buoy_id'], value=json.dumps({k: v for k, v in payload.items() if pd.notna(v)}))
                except Exception: continue
            producer.flush()
            print(f"✅ Inviati {len(df)} record fisici dalla NOAA.")
        except Exception as e: print(f"🔥 Errore nel producer NOAA: {e}")
        time.sleep(1800)

if __name__ == "__main__": main()
