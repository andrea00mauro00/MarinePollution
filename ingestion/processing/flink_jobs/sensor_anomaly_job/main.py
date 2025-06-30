import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema

def detect_anomaly(record_str: str):
    """
    Analizza un record JSON. Restituisce una tupla: (è_anomalia, record_convertito).
    Restituisce (False, None) se il record non è anomalo o non è valido.
    """
    try:
        rec = json.loads(record_str)
        is_anomaly = False
        if rec.get('pH') is not None and (rec['pH'] < 6.5 or rec['pH'] > 8.5):
            is_anomaly = True
        if rec.get('turbidity', 0) > 5:
            is_anomaly = True
        if rec.get('contaminant_ppm', 0) > 7:
            is_anomaly = True
        
        if is_anomaly:
            return True, rec
        else:
            return False, None
            
    except (json.JSONDecodeError, TypeError):
        return False, None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Sources
    buoy_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_topics('buoy_raw') \
        .set_group_id('sensor-anomaly-group-buoy') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    water_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_topics('water_metrics_raw') \
        .set_group_id('sensor-anomaly-group-water') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    buoy_stream = env.from_source(buoy_source, WatermarkStrategy.no_watermarks(), 'Buoy Kafka Source')
    water_stream = env.from_source(water_source, WatermarkStrategy.no_watermarks(), 'Water Metrics Kafka Source')

    combined_stream = buoy_stream.union(water_stream)

    # Filtra solo le anomalie - qui usiamo filter anziché map+filter per semplificare
    def is_anomaly(record_str):
        try:
            rec = json.loads(record_str)
            
            if rec.get('pH') is not None and (rec['pH'] < 6.5 or rec['pH'] > 8.5):
                return True
            
            if rec.get('turbidity', 0) > 5:
                return True
                
            if rec.get('contaminant_ppm', 0) > 7:
                return True
                
            return False
        except:
            return False
    
    anomalies_stream = combined_stream.filter(is_anomaly)

    # Kafka Sink per alert
    kafka_alerts_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("sensor_alerts")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
        
    anomalies_stream.sink_to(kafka_alerts_sink).name("Kafka Alerts Sink")

    env.execute('Sensor Anomaly Detection Job')

if __name__ == '__main__':
    main()