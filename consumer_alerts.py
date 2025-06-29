#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys

def main():
    consumer = KafkaConsumer(
        'sensor_alerts',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='alert-consumer-group',
        value_deserializer=lambda b: b.decode('utf-8')
    )

    print("🚀 In ascolto su 'sensor_alerts'… premi Ctrl+C per uscire.")
    try:
        for msg in consumer:
            try:
                data = json.loads(msg.value)
            except json.JSONDecodeError:
                data = msg.value
            print(f"[{msg.topic}@{msg.partition}:{msg.offset}]  {data}")
    except KeyboardInterrupt:
        print("\n👋 Arrivederci!")
        sys.exit(0)

if __name__=='__main__':
    main()
