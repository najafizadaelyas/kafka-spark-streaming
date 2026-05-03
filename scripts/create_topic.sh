#!/usr/bin/env bash
# Create the Kafka topic used by the pipeline.
# Usage: ./scripts/create_topic.sh [bootstrap-servers] [topic] [partitions]

BOOTSTRAP=${1:-localhost:9092}
TOPIC=${2:-user-events}
PARTITIONS=${3:-3}
REPLICATION=${4:-1}

docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

echo "Topic '$TOPIC' ready."
