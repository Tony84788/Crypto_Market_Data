import json
import logging
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.serializer import Deserializer

from src.kafka.processor import (
    process_crypto_event,
    validate_crypto_event,
)
from src.kafka.dlq import create_dlq_producer, send_to_dlq
from src.lake.storage import save_raw_event


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-market-data"


class StringDeserializer(Deserializer):
    """Deserialize UTF-8 bytes into Python strings."""

    def deserialize(self, topic, data):
        if data is None:
            return None

        return data.decode("utf-8")


class JSONDeserializer(Deserializer):
    """Deserialize JSON bytes into Python objects."""

    def deserialize(self, topic, data):
        if data is None:
            return None

        return json.loads(data.decode("utf-8"))


def create_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer."""

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="crypto-market-dlq-test",
        key_deserializer=StringDeserializer(),
        value_deserializer=JSONDeserializer(),
    )

    return consumer


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    logging.getLogger("kafka").setLevel(logging.WARNING)

    print("Kafka consumer starting...")
    print(f"Listening to topic: {KAFKA_TOPIC}")

    consumer = create_consumer()
    dlq_producer = create_dlq_producer()

    try:
        for message in consumer:

            print("\n" + "=" * 60)
            print("KAFKA MESSAGE")
            print("=" * 60)

            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Key: {message.key}")

            event = message.value

            envelope_errors = []

            required_envelope_fields = [
                "event_id",
                "event_type",
                "source",
                "received_at",
                "data",
            ]

            # Validate the envelope.
            for field in required_envelope_fields:
                if field not in event:
                    envelope_errors.append(
                        f"Missing envelope field: {field}"
                    )

            # Handle envelope validation AFTER checking all fields.
            if envelope_errors:
                print("Event rejected.")

                send_to_dlq(
                    dlq_producer,
                    event,
                    envelope_errors,
                )

                continue

            crypto_data = event["data"]

            # Validate the cryptocurrency data.
            errors = validate_crypto_event(
                crypto_data
            )

            if errors:
                print("Event rejected.")

                send_to_dlq(
                    dlq_producer,
                    event,
                    errors,
                )

                continue

            # Process the valid cryptocurrency event.
            processed_event = process_crypto_event(
                crypto_data
            )

            lake_path = save_raw_event(event)

            print(
                f"Saved raw event to data lake: {lake_path}"
            )

            print("\nEVENT METADATA")

            print(
                json.dumps(
                    {
                        "event_id": event["event_id"],
                        "event_type": event["event_type"],
                        "source": event["source"],
                        "received_at": event["received_at"],
                        "kafka": {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                        },
                    },
                    indent=2,
                )
            )

            print("\nPROCESSED EVENT")

            print(
                json.dumps(
                    processed_event,
                    indent=2,
                )
            )

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        dlq_producer.close()

        print("\nKafka consumer closed.")
        print("DLQ producer closed.")


if __name__ == "__main__":
    main()
def consume_batch(max_messages: int = 3, timeout_seconds: int = 30) -> int:
    """
    Consume a bounded batch of cryptocurrency events.

    This function is designed for Airflow tasks:
    it processes up to max_messages and then exits.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    logging.getLogger("kafka").setLevel(logging.WARNING)

    print("Kafka batch consumer starting...")
    print(f"Target messages: {max_messages}")

    consumer = create_consumer()
    dlq_producer = create_dlq_producer()

    processed_count = 0

    try:
        while processed_count < max_messages:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=max_messages - processed_count,
            )

            if not records:
                timeout_seconds -= 1

                if timeout_seconds <= 0:
                    print(
                        f"Timed out after processing "
                        f"{processed_count} messages."
                    )
                    break

                continue

            for messages in records.values():
                for message in messages:

                    print("\n" + "=" * 60)
                    print("KAFKA MESSAGE")
                    print("=" * 60)

                    print(f"Partition: {message.partition}")
                    print(f"Offset: {message.offset}")
                    print(f"Key: {message.key}")

                    event = message.value

                    envelope_errors = []

                    required_envelope_fields = [
                        "event_id",
                        "event_type",
                        "source",
                        "received_at",
                        "data",
                    ]

                    for field in required_envelope_fields:
                        if field not in event:
                            envelope_errors.append(
                                f"Missing envelope field: {field}"
                            )

                    if envelope_errors:
                        print("Event rejected.")

                        send_to_dlq(
                            dlq_producer,
                            event,
                            envelope_errors,
                        )

                        processed_count += 1
                        continue

                    crypto_data = event["data"]

                    errors = validate_crypto_event(
                        crypto_data
                    )

                    if errors:
                        print("Event rejected.")

                        send_to_dlq(
                            dlq_producer,
                            event,
                            errors,
                        )

                        processed_count += 1
                        continue

                    processed_event = process_crypto_event(
                        crypto_data
                    )

                    lake_path = save_raw_event(event)

                    print(
                        f"Saved raw event to data lake: "
                        f"{lake_path}"
                    )

                    print("\nPROCESSED EVENT")

                    print(
                        json.dumps(
                            processed_event,
                            indent=2,
                        )
                    )

                    processed_count += 1

                    if processed_count >= max_messages:
                        break

                if processed_count >= max_messages:
                    break

        consumer.commit()

        print(
            f"\nKafka batch consumer finished. "
            f"Processed {processed_count} messages."
        )

        return processed_count

    finally:
        consumer.close()
        dlq_producer.close()

        print("Kafka consumer closed.")
        print("DLQ producer closed.")


def process_kafka_message(
    message,
    dlq_producer,
) -> bool:
    """
    Validate, process, and save one Kafka cryptocurrency message.

    Returns True when the message has been handled successfully
    or sent to the DLQ.
    """

    print("\n" + "=" * 60)
    print("KAFKA MESSAGE")
    print("=" * 60)

    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")

    event = message.value

    required_envelope_fields = [
        "event_id",
        "event_type",
        "source",
        "received_at",
        "data",
    ]

    envelope_errors = [
        f"Missing envelope field: {field}"
        for field in required_envelope_fields
        if field not in event
    ]

    if envelope_errors:
        print("Event rejected.")

        send_to_dlq(
            dlq_producer,
            event,
            envelope_errors,
        )

        return True

    crypto_data = event["data"]

    errors = validate_crypto_event(
        crypto_data
    )

    if errors:
        print("Event rejected.")

        send_to_dlq(
            dlq_producer,
            event,
            errors,
        )

        return True

    processed_event = process_crypto_event(
        crypto_data
    )

    lake_path = save_raw_event(event)

    print(
        f"Saved raw event to data lake: "
        f"{lake_path}"
    )

    print("\nPROCESSED EVENT")

    print(
        json.dumps(
            processed_event,
            indent=2,
        )
    )

    return True


def consume_kafka_offsets(
    message_metadata: list[dict],
    timeout_seconds: int = 30,
) -> int:
    """
    Consume only the Kafka messages identified by topic,
    partition, and offset metadata.

    Designed for Airflow so each DAG run processes only
    the messages produced by its preceding producer task.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    logging.getLogger("kafka").setLevel(
        logging.WARNING
    )

    if not message_metadata:
        raise ValueError(
            "No Kafka message metadata was provided."
        )

    print("Kafka Airflow consumer starting...")

    print(
        f"Target messages: "
        f"{len(message_metadata)}"
    )

    print(
        f"Timeout: "
        f"{timeout_seconds} seconds"
    )

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        group_id=None,
        key_deserializer=StringDeserializer(),
        value_deserializer=JSONDeserializer(),
    )

    dlq_producer = create_dlq_producer()

    target_offsets = {
        (
            item["partition"],
            item["offset"],
        )
        for item in message_metadata
    }

    topic_partitions = [
        TopicPartition(
            KAFKA_TOPIC,
            partition,
        )
        for partition, _ in target_offsets
    ]

    processed_offsets = set()

    try:
        consumer.assign(topic_partitions)

        for topic_partition in topic_partitions:

            matching_offsets = [
                offset
                for partition, offset in target_offsets
                if partition == topic_partition.partition
            ]

            consumer.seek(
                topic_partition,
                min(matching_offsets),
            )

        start_time = time.monotonic()

        while processed_offsets != target_offsets:

            elapsed = (
                time.monotonic()
                - start_time
            )

            if elapsed >= timeout_seconds:
                missing_offsets = (
                    target_offsets
                    - processed_offsets
                )

                raise TimeoutError(
                    "Timed out waiting for Kafka "
                    f"messages. Missing offsets: "
                    f"{sorted(missing_offsets)}"
                )

            records = consumer.poll(
                timeout_ms=1000,
                max_records=len(
                    target_offsets
                ),
            )

            if not records:
                continue

            for messages in records.values():

                for message in messages:

                    message_key = (
                        message.partition,
                        message.offset,
                    )

                    if (
                        message_key
                        not in target_offsets
                    ):
                        continue

                    process_kafka_message(
                        message,
                        dlq_producer,
                    )

                    processed_offsets.add(
                        message_key
                    )

        print(
            "\nKafka Airflow consumer finished."
        )

        print(
            f"Processed "
            f"{len(processed_offsets)} "
            f"target messages."
        )

        return len(processed_offsets)

    finally:
        consumer.close()
        dlq_producer.close()

        print(
            "Kafka Airflow consumer closed."
        )

        print(
            "DLQ producer closed."
        )