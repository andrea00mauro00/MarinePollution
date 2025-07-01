import json
import os
import sys
import types
import pytest

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Stub pyflink modules required by the module under test
pyflink = types.ModuleType("pyflink")
sys.modules.setdefault("pyflink", pyflink)
sys.modules.setdefault("pyflink.common", types.ModuleType("pyflink.common"))
sys.modules.setdefault("pyflink.common.serialization", types.ModuleType("pyflink.common.serialization"))
sys.modules.setdefault("pyflink.common.typeinfo", types.ModuleType("pyflink.common.typeinfo"))
sys.modules.setdefault("pyflink.common.watermark_strategy", types.ModuleType("pyflink.common.watermark_strategy"))
sys.modules.setdefault("pyflink.datastream", types.ModuleType("pyflink.datastream"))
sys.modules.setdefault("pyflink.datastream.connectors.kafka", types.ModuleType("pyflink.datastream.connectors.kafka"))
sys.modules["pyflink.common.serialization"].SimpleStringSchema = object
sys.modules["pyflink.common.typeinfo"].Types = object
sys.modules["pyflink.common.watermark_strategy"].WatermarkStrategy = object
sys.modules["pyflink.datastream"].StreamExecutionEnvironment = object
sys.modules["pyflink.datastream.connectors.kafka"].KafkaSource = object
sys.modules["pyflink.datastream.connectors.kafka"].KafkaSink = object
sys.modules["pyflink.datastream.connectors.kafka"].KafkaRecordSerializationSchema = object

from ingestion.processing.flink_jobs.sensor_anomaly_job.main import detect_anomaly


def test_valid_record_normal():
    record = json.dumps({"pH": 7.2, "turbidity": 3, "contaminant_ppm": 2})
    assert detect_anomaly(record) == (False, None)


@pytest.mark.parametrize(
    "record",
    [
        json.dumps({"pH": 9.0, "turbidity": 3, "contaminant_ppm": 2}),
        json.dumps({"pH": 7.0, "turbidity": 6, "contaminant_ppm": 2}),
        json.dumps({"pH": 7.0, "turbidity": 3, "contaminant_ppm": 8}),
    ],
)
def test_anomalous_records(record):
    is_anomaly, rec_dict = detect_anomaly(record)
    assert is_anomaly is True
    assert rec_dict == json.loads(record)
