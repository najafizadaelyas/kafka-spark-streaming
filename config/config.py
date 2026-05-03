"""
Centralised configuration — reads from environment variables with sensible defaults.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str             = os.getenv("KAFKA_TOPIC", "user-events")
    group_id: str          = os.getenv("KAFKA_GROUP_ID", "spark-streaming-group")
    num_partitions: int    = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


@dataclass(frozen=True)
class SparkConfig:
    app_name: str         = os.getenv("SPARK_APP_NAME", "KafkaSparkDeltaStreaming")
    master: str           = os.getenv("SPARK_MASTER", "local[*]")
    trigger_interval: str = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")


@dataclass(frozen=True)
class DeltaConfig:
    output_path: str     = os.getenv("DELTA_OUTPUT_PATH", "/tmp/delta/user-events")
    checkpoint_path: str = os.getenv("CHECKPOINT_PATH",   "/tmp/checkpoints/user-events")
    retention_hours: int = int(os.getenv("DELTA_RETENTION_HOURS", "168"))


kafka = KafkaConfig()
spark = SparkConfig()
delta = DeltaConfig()
