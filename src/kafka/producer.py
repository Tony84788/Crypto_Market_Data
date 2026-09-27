import json

from kafka import KafkaProducer, producer
from kafka.serializer import Serializer

from src.ingestion.coingecko_client import CoinGeckoClient
from src.kafka.envelope import create_event_envelope


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-market-data"


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


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=StringSerializer(),
        value_serializer=JSONSerializer(),
    )


def send_crypto_data(
    producer: KafkaProducer,
    crypto_data: list[dict],
) -> list:
    """Send cryptocurrency records to Kafka and return metadata."""

    futures = []

    for record in crypto_data:
        event = create_event_envelope(record)

        future = producer.send(
            KAFKA_TOPIC,
            key=record["id"],
            value=event,
        )

        futures.append(future)

        print(
            f"Sent: {record['id']} "
            f"({record['symbol']}) "
            f"with key='{record['id']}'"
        )

    producer.flush()

    return [future.get(timeout=10) for future in futures]


def main():
    print("Starting CoinGecko → Kafka pipeline...")

    client = CoinGeckoClient()

    crypto_data = client.get_market_data(
        coin_ids=[
            "bitcoin",
            "ethereum",
            "solana",
        ]
    )

    print(
        f"Retrieved {len(crypto_data)} "
        "cryptocurrencies from CoinGecko"
    )

    producer = create_producer()

    try:
        send_crypto_data(
            producer,
            crypto_data,
        )

    finally:
        producer.close()

    print(
        f"Successfully sent {len(crypto_data)} "
        f"records to Kafka topic '{KAFKA_TOPIC}'"
    )


if __name__ == "__main__":
    main()