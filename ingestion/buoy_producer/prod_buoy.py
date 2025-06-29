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
logger = logging.getLogger("buoy-producer")

# Kafka Producer
producer = Producer({"bootstrap.servers": cfg["kafka"]["brokers"]})

# Carica schema solo per validazione locale
schema_str = open("buoy_schema.json").read()

def generate_reading():
    return {
        "buoy_id":       "buoy-001",
        "timestamp":     datetime.utcnow().isoformat() + "Z",
        "latitude":      45.0 + random.random()/10,
        "longitude":     12.0 + random.random()/10,
        "pH":            round(7 + random.uniform(-0.2,0.2),2),
        "turbidity":     round(3 + random.uniform(0,1),2),
        "contaminant_ppm": round(random.uniform(0,5),2)
    }

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    topic = cfg["kafka"]["topic"]
    logger.info(f"Starting buoy producer, sending to topic '{topic}'")
    while True:
        record = generate_reading()
        validate(record, json.loads(schema_str))
        producer.produce(topic, value=json.dumps(record).encode("utf-8"), callback=delivery_report)
        producer.poll(0)
        time.sleep(cfg["producer"]["interval_sec"])
