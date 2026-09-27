import json
import logging

from kafka import KafkaProducer
from kafka.serializer import Serializer


logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DLQ_TOPIC = "crypto-market-data-dlq"


class StringSerializer(Serializer):
    """Serialize Python strings into UTF-8 bytes."""

    def serialize(self, topic, data):
        if data is None:
            return None

        return data.encode("utf-8")


class JSONSerializer(Serializer):
    """Serialize Python dictionaries into JSON bytes."""

    def serialize(self, topic, data):
        if data is None:
            return None

        return json.dumps(data).encode("utf-8")


def create_dlq_producer() -> KafkaProducer:
    """Create and return a Kafka producer for the DLQ."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=StringSerializer(),
        value_serializer=JSONSerializer(),
    )


def send_to_dlq(
    producer: KafkaProducer,
    event: dict,
    errors: list[str],
) -> None:
    """Send an invalid event and its validation errors to the DLQ."""

    dlq_event = {
        "original_event": event,
        "errors": errors,
    }

    producer.send(
        DLQ_TOPIC,
        key=event.get("id"),
        value=dlq_event,
    )

    producer.flush()

    logger.warning(
        "Event sent to DLQ: %s",
        event.get("id"),
    )
