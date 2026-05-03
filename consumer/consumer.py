"""
Kafka Consumer — reads events from a topic and prints them (useful for debugging / monitoring).
In production the Spark job is the primary consumer; this is a lightweight inspector.
"""
import json
import logging
import os
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run(bootstrap_servers: str, topic: str, group_id: str = "debug-consumer"):
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    logger.info("Subscribed to topic=%s  group=%s", topic, group_id)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of partition %d", msg.partition())
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue

            event = json.loads(msg.value().decode("utf-8"))
            logger.info("Received [partition=%d offset=%d]: %s", msg.partition(), msg.offset(), event)
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "user-events"),
    )
