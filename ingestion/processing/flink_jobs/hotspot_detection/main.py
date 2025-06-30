import json
import math
from datetime import datetime
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcola la distanza in km tra due punti usando la formula di Haversine
    """
    R = 6371.0  # raggio della Terra in km
    
    # Converti da gradi a radianti
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Calcola differenze
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    # Formula di Haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Distanza in km
    distance = R * c
    return distance

def get_coordinates(sensor_id):
    """Restituisce le coordinate per un sensore dato il suo ID."""
    sensor_locations = {
        '13002': (42.345, -68.502),  # Boa NOAA Atlantico
        '09380000': (36.865, -111.588),  # USGS Colorado River
    }
    
    return sensor_locations.get(sensor_id, (0, 0))

class HotspotDetector(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        # Raggruppa anomalie per vicinanza geografica (distanza < 50km)
        anomalies = list(elements)
        clusters = []
        processed = set()
        
        for i, anomaly in enumerate(anomalies):
            if i in processed:
                continue
                
            # Inizia un nuovo cluster
            cluster = [anomaly]
            processed.add(i)
            
            sensor_id, lat, lon, metric, value = anomaly
            
            # Trova punti vicini
            for j, (s_id, s_lat, s_lon, s_metric, s_value) in enumerate(anomalies):
                if j in processed:
                    continue
                    
                if haversine_distance(lat, lon, s_lat, s_lon) < 50.0:  # 50 km di distanza
                    cluster.append((s_id, s_lat, s_lon, s_metric, s_value))
                    processed.add(j)
            
            if len(cluster) >= 3:  # Almeno 3 anomalie per definire un hotspot
                clusters.append(cluster)
        
        # Converti clusters in hotspots
        for cluster in clusters:
            # Calcola centro del cluster
            avg_lat = sum(item[1] for item in cluster) / len(cluster)
            avg_lon = sum(item[2] for item in cluster) / len(cluster)
            
            # Conta i tipi di metrica
            metrics_count = {}
            for _, _, _, metric, _ in cluster:
                metrics_count[metric] = metrics_count.get(metric, 0) + 1
            
            # Determina la metrica dominante
            dominant_metric = max(metrics_count.items(), key=lambda x: x[1])[0]
            
            # Converti in hotspot
            hotspot = {
                'lat': avg_lat,
                'lon': avg_lon,
                'radius_km': max(haversine_distance(avg_lat, avg_lon, item[1], item[2]) for item in cluster),
                'anomaly_count': len(cluster),
                'metrics': {metric: count for metric, count in metrics_count.items()},
                'sensors': list(set(item[0] for item in cluster)),
                'severity': 'high' if len(cluster) > 5 else 'medium',
                'timestamp': datetime.now().isoformat()
            }
            out.collect(json.dumps(hotspot))

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka Source per le anomalie
    anomaly_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_topics('sensor_alerts') \
        .set_group_id('hotspot-detection-job') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    anomalies_stream = env.from_source(anomaly_source, WatermarkStrategy.no_watermarks(), 'Anomalies Source')

    # Estrai informazioni rilevanti dalle anomalie
    def extract_anomaly_info(record_str):
        try:
            rec = json.loads(record_str)
            sensor_id = rec.get('sensor_id', 'unknown')
            
            lat, lon = get_coordinates(sensor_id)
            
            # Determina il tipo di anomalia
            if 'pH' in rec and (rec['pH'] < 6.5 or rec['pH'] > 8.5):
                metric, value = 'pH', rec['pH']
            elif 'turbidity' in rec and rec['turbidity'] > 5:
                metric, value = 'turbidity', rec['turbidity']
            elif 'contaminant_ppm' in rec and rec['contaminant_ppm'] > 7:
                metric, value = 'contaminant_ppm', rec['contaminant_ppm']
            else:
                return None
                
            return (sensor_id, lat, lon, metric, value)
        except:
            return None

    anomaly_info_stream = anomalies_stream \
        .map(lambda x: extract_anomaly_info(x), 
             output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.DOUBLE()])) \
        .filter(lambda x: x is not None)

    # Applica finestra scorrevole e rileva hotspot
    hotspots_stream = anomaly_info_stream \
        .key_by(lambda x: x[3]) \
        .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10))) \
        .process(HotspotDetector(), output_type=Types.STRING())

    # Kafka Sink per hotspot
    hotspots_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:29092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("pollution_hotspots")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
        
    hotspots_stream.sink_to(hotspots_sink).name("Pollution Hotspots Sink")

    env.execute('Pollution Hotspot Detection Job')

if __name__ == '__main__':
    main()