import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema

def detect_anomaly(record_str: str) -> tuple[bool, dict | None]:
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
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('buoy_raw') \
        .set_group_id('sensor-anomaly-group-buoy') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    water_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('water_metrics_raw') \
        .set_group_id('sensor-anomaly-group-water') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    buoy_stream = env.from_source(buoy_source, WatermarkStrategy.no_watermarks(), 'Buoy Kafka Source')
    water_stream = env.from_source(water_source, WatermarkStrategy.no_watermarks(), 'Water Metrics Kafka Source')

    combined_stream = buoy_stream.union(water_stream)

    # Rileva anomalie e mantiene sia la stringa originale che il dizionario
    # CORREZIONE: Usa Types.PICKLED_BYTE_ARRAY() per rappresentare il dizionario Python generico.
    anomalies_data_stream = combined_stream \
        .map(lambda record_str: (record_str, detect_anomaly(record_str)),
             output_type=Types.TUPLE([Types.STRING(), Types.TUPLE([Types.BOOLEAN(), Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())])])) \
        .filter(lambda x: x[1][0])

    # Estrai solo la stringa originale per il sink Kafka
    anomalies_string_stream = anomalies_data_stream.map(lambda x: x[0], output_type=Types.STRING())

    # Kafka Sink per alert
    kafka_alerts_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("sensor_alerts")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    anomalies_string_stream.sink_to(kafka_alerts_sink).name("Kafka Alerts Sink")

    # --- LOGICA CORRETTA PER JDBC SINK ---
    
    # 1. Definisci una funzione che trasforma il dizionario dell'anomalia in una tupla semplice
    def anomaly_to_tuple(data: dict):
        sensor_id = data.get('sensor_id', 'unknown')
        if 'pH' in data and (data['pH'] < 6.5 or data['pH'] > 8.5):
            return sensor_id, 'pH', float(data['pH'])
        elif 'turbidity' in data and data['turbidity'] > 5:
            return sensor_id, 'turbidity', float(data['turbidity'])
        elif 'contaminant_ppm' in data and data['contaminant_ppm'] > 7:
            return sensor_id, 'contaminant_ppm', float(data['contaminant_ppm'])
        return None, None, None

    # 2. Crea uno stream di tuple, scartando quelle non valide
    jdbc_stream = anomalies_data_stream \
        .map(lambda x: anomaly_to_tuple(x[1][1]), 
             output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE()])) \
        .filter(lambda x: x[0] is not None)

    # 3. Definisci il sink per le tuple, senza bisogno di una funzione manuale
    jdbc_sink = JdbcSink.sink(
        "INSERT INTO anomalies (sensor_id, metric, value) VALUES (?, ?, ?)",
        Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE()]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:postgresql://postgres:5432/marine")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name("flink")
            .with_password("flinkpw")
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(1)
            .with_max_retries(3)
            .build()
    )
    
    jdbc_stream.add_sink(jdbc_sink).name("PostgreSQL Anomalies Sink")

    env.execute('Sensor Anomaly Detection Job')

if __name__ == '__main__':
    main()

