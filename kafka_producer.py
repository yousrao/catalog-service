
import json
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "ecommerce.events"


def publish_product_consulted(product_id: int):
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    event = {
        "eventId": str(uuid.uuid4()),
        "eventType": "ProductConsulted",
        "source": "catalog",
        "productId": product_id,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    try:
        producer.send(
            TOPIC,
            key=str(product_id).encode("utf-8"),
            value=event
        ).get(timeout=10)

        return event
    finally:
        producer.close()
