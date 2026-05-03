"""
Kafka Producer — publishes sample events to a Kafka topic.
Each message is validated against the Avro/JSON schema before sending.
"""
import json
import time
import random
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from schemas.event_schema import validate_event, EVENT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        logger.info("Record delivered to %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset())


def build_event(user_id: int) -> dict:
    return {
        "event_id": f"evt-{random.randint(100000, 999999)}",
        "user_id": user_id,
        "event_type": random.choice(["click", "view", "purchase", "add_to_cart"]),
        "product_id": f"prod-{random.randint(1, 500)}",
        "amount": round(random.uniform(1.0, 500.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(bootstrap_servers: str, topic: str, rate_per_second: int = 5):
    conf = {"bootstrap.servers": bootstrap_servers}
    producer = Producer(conf)

    logger.info("Starting producer → topic=%s  rate=%d msg/s", topic, rate_per_second)
    try:
        while True:
            event = build_event(user_id=random.randint(1, 1000))
            validate_event(event)  # schema enforcement before publish
            producer.produce(
                topic,
                key=event["user_id"].to_bytes(4, "big") if isinstance(event["user_id"], int) else event["user_id"].encode(),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(1 / rate_per_second)
    except KeyboardInterrupt:
        logger.info("Shutting down producer…")
    finally:
        producer.flush()


if __name__ == "__main__":
    import os
    run(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "user-events"),
        rate_per_second=int(os.getenv("PRODUCER_RATE", "5")),
    )
