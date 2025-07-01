import time, json, pandas as pd, yaml, copernicusmarine as cm, schedule, socket, os
from confluent_kafka import Producer
def load_config():
    with open('/app/config.yml') as f: return yaml.safe_load(f)
def create_producer(config):
    return Producer({'bootstrap.servers': config['kafka']['brokers'], 'client.id': socket.gethostname()})
def job():
    config, producer = load_config(), create_producer(config)
    dataset_id, topic = config['cmems']['biochem']['dataset_id'], config['kafka']['topics']['biochem']
    variables = ["ph", "dic", "alk", "o2", "no3", "po4", "si"]
    print(f"🚀 Eseguo il job giornaliero per i dati biochimici...")
    try:
        start_time = pd.to_datetime('now', utc=True)
        end_time = start_time + pd.Timedelta(days=1)
        ds = cm.read_dataframe(dataset_id=dataset_id, variables=variables, start_datetime=start_time.isoformat(), end_datetime=end_time.isoformat())
        if ds.empty: print("INFO: Nessun dato biochimico trovato."); return
        for _, row in ds.iterrows():
            payload = {"model_run_id": f"run_{start_time.strftime('%Y%m%d')}", "timestamp": row['time'].isoformat(), "latitude": row['latitude'], "longitude": row['longitude'], "depth": row['depth']}
            for var in variables:
                if var in row and pd.notna(row[var]): payload[var] = row[var]
            producer.produce(topic, key=payload['model_run_id'], value=json.dumps(payload, default=str))
        producer.flush()
        print(f"✅ Inviati {len(ds)} record biochimici.")
    except Exception as e: print(f"🔥 Errore nel job biochimico: {e}")
if __name__ == "__main__":
    cm.login(username=os.getenv("COPERNICUS_USERNAME"), password=os.getenv("COPERNICUS_PASSWORD"))
    config = load_config()
    cron_time = config['cmems']['biochem']['schedule']
    print(f"🕰️ Producer biochimico avviato. Esecuzione programmata ogni giorno alle {cron_time} UTC.")
    schedule.every().day.at(cron_time, "UTC").do(job)
    print("INFO: Eseguo il job una volta all'avvio per il test...")
    job()
    while True:
        schedule.run_pending()
        time.sleep(60)
