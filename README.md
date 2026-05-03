# ⚡ kafka-spark-streaming

> Real-time streaming pipeline — **Kafka · PySpark · Delta Lake** with schema enforcement

---

## Architecture

```
┌──────────────┐    Kafka     ┌──────────────────────────┐    Delta Lake
│   Producer   │ ──────────► │  PySpark Structured      │ ───────────►  /delta/user-events
│ (Python)     │  user-events │  Streaming Job           │               (Parquet + transaction log)
└──────────────┘              │  • JSON parse            │
                              │  • Schema enforcement    │
                              │  • Type coercion         │
                              └──────────────────────────┘
```

### Components

| Component | Description |
|---|---|
| `producer/` | Confluent Kafka producer — generates & validates user-event messages |
| `consumer/` | Lightweight debug consumer (inspect topic messages) |
| `spark_streaming/` | PySpark Structured Streaming job — Kafka → Delta Lake |
| `delta_lake/` | Delta utility helpers (compaction, vacuum, time-travel) |
| `schemas/` | Event schema (JSON Schema + PySpark StructType) — single source of truth |
| `config/` | Centralised config via environment variables |
| `tests/` | Pytest unit tests |

---

## Quick Start

### 1. Start infrastructure

```bash
docker-compose up -d
```

This starts: **Zookeeper**, **Kafka**, **Kafka UI** (http://localhost:8080), **Spark master**.

### 2. Create the Kafka topic

```bash
./scripts/create_topic.sh
```

### 3. Start the producer

```bash
pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m producer.producer
```

### 4. Run the Spark streaming job

```bash
./scripts/run_spark_job.sh
```

### 5. Query the Delta table (PySpark shell)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("/tmp/delta/user-events")
df.show()
```

---

## Schema Enforcement

All events must conform to the `UserEvent` schema defined in `schemas/event_schema.py`:

```json
{
  "event_id":   "evt-<digits>",
  "user_id":    <integer ≥ 1>,
  "event_type": "click | view | purchase | add_to_cart",
  "product_id": "prod-<digits>",
  "amount":     <number ≥ 0>,
  "timestamp":  "<ISO-8601 date-time>"
}
```

- The **producer** validates before publishing (fail-fast).
- The **Spark job** silently drops malformed rows and logs them.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker(s) |
| `KAFKA_TOPIC` | `user-events` | Topic name |
| `DELTA_OUTPUT_PATH` | `/tmp/delta/user-events` | Delta table output path |
| `CHECKPOINT_PATH` | `/tmp/checkpoints/user-events` | Spark checkpoint dir |
| `PRODUCER_RATE` | `5` | Events per second |

---

## Tests

```bash
pytest tests/ -v
```

---

## Tech Stack

- **Apache Kafka** 3.x (via Confluent Platform 7.6)
- **Apache Spark** 3.5 + Structured Streaming
- **Delta Lake** 3.0
- **Python** 3.11
- **Docker Compose** for local infrastructure
