import time
import json
import random
from datetime import datetime
import yaml
import logging
from confluent_kafka import Producer
from jsonschema import validate

# Carica config
with open("/app/config.yml") as f:
    cfg = yaml.safe_load(f)

# Setup logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    level=cfg["logging"]["level"]
)
logger = logging.getLogger("water-metrics-producer")

# Kafka Producer
producer = Producer({"bootstrap.servers": cfg["kafka"]["brokers"]})

# Carica schema solo per validazione locale
schema_str = open("water_metrics_schema.json").read()

def generate_metric():
    return {
        "station_id":      f"station-{random.randint(1,5):03}",
        "timestamp":       datetime.utcnow().isoformat() + "Z",
        "pH":              round(7 + random.uniform(-0.3,0.3),2),
        "turbidity":       round(2 + random.uniform(0,1.5),2),
        "contaminant_ppm": round(random.uniform(0,10),2),
        "temperature":     round(15 + random.uniform(-2,2),2),
        "oxygen":          round(8 + random.uniform(-1,1),2)
    }

def delivery(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    topic = cfg["kafka"]["topic"]
    logger.info(f"Starting water-metrics producer on topic '{topic}'")
    while True:
        rec = generate_metric()
        validate(rec, json.loads(schema_str))
        producer.produce(topic, value=json.dumps(rec).encode("utf-8"), callback=delivery)
        producer.poll(0)
        time.sleep(cfg["producer"]["interval_sec"])
