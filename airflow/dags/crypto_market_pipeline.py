import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract_from_coingecko():
    from src.ingestion.coingecko_client import CoinGeckoClient

    print("Starting CoinGecko extraction...")

    client = CoinGeckoClient()

    crypto_data = client.get_market_data(
        coin_ids=[
            "bitcoin",
            "ethereum",
            "solana",
        ]
    )

    record_count = len(crypto_data)

    print(f"Retrieved {record_count} cryptocurrencies from CoinGecko.")

    if record_count == 0:
        raise ValueError("Monitoring check failed: CoinGecko returned 0 records.")

    print(f"Monitoring check passed: {record_count} records retrieved.")

    return crypto_data


def produce_to_kafka(**context):
    from src.kafka.producer import (
        create_producer,
        send_crypto_data,
    )

    print("Starting Kafka producer...")

    crypto_data = context["ti"].xcom_pull(
        task_ids="extract_from_coingecko"
    )

    if not crypto_data:
        raise ValueError(
            "No cryptocurrency data received "
            "from extract_from_coingecko."
        )

    print(
        f"Received {len(crypto_data)} "
        "records from extraction task."
    )

    producer = create_producer()

    try:
        metadata = send_crypto_data(
            producer,
            crypto_data,
        )
    finally:
        producer.close()

    message_metadata = [
        {
            "partition": record.partition,
            "offset": record.offset,
        }
        for record in metadata
    ]

    print(
        "Kafka messages successfully produced:"
    )

    for item in message_metadata:
        print(
            f"Partition={item['partition']}, "
            f"Offset={item['offset']}"
        )

    return message_metadata


def consume_from_kafka(**context):
    from src.kafka.consumer import consume_kafka_offsets

    print("Starting bounded Kafka consumer...")

    message_metadata = context["ti"].xcom_pull(
        task_ids="produce_to_kafka"
    )

    if not message_metadata:
        raise ValueError(
            "No Kafka message metadata received "
            "from produce_to_kafka."
        )

    print(
        f"Received {len(message_metadata)} "
        "Kafka target messages."
    )

    processed_count = consume_kafka_offsets(
        message_metadata=message_metadata,
        timeout_seconds=30,
    )

    print(
        f"Kafka consumer processed "
        f"{processed_count} target messages."
    )

    return processed_count


def transform_to_parquet():
    from src.lake.transform import (
        get_raw_events,
        transform_events,
        save_processed_data,
    )

    print("Starting lake transformation...")

    events = get_raw_events()

    print(
        f"Loaded {len(events)} raw events."
    )

    df = transform_events(events)

    print(
        f"Transformed {len(df)} records."
    )

    parquet_path = save_processed_data(df)

    print(
        f"Parquet file created: {parquet_path}"
    )

    return str(parquet_path)


def load_to_bigquery(**context):
    from src.bigquery.loader import (
        load_parquet_to_bigquery,
    )

    print("Starting BigQuery load...")

    parquet_path = context["ti"].xcom_pull(
        task_ids="transform_to_parquet"
    )

    if not parquet_path:
        raise ValueError(
            "No Parquet path received from "
            "transform_to_parquet."
        )

    print(
        f"Parquet file received: {parquet_path}"
    )

    load_parquet_to_bigquery(parquet_path)

    print(
        "BigQuery load completed successfully."
    )


def run_dbt():
    import subprocess

    dbt_project = (
        "/home/tony/Desktop/CryptoData/crypto_dbt"
    )

    print("Starting dbt build...")

    result = subprocess.run(
        ["dbt", "build"],
        cwd=dbt_project,
        check=True,
        text=True,
    )

    print("dbt build completed successfully.")

    return result.returncode


with DAG(
    dag_id="crypto_market_pipeline",
    description="Cryptocurrency market data pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["crypto", "kafka", "bigquery", "dbt"],
) as dag:

    extract = PythonOperator(
        task_id="extract_from_coingecko",
        python_callable=extract_from_coingecko,
    )

    produce = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=produce_to_kafka,
    )

    consume = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_from_kafka,
    )

    transform = PythonOperator(
        task_id="transform_to_parquet",
        python_callable=transform_to_parquet,
    )

    load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    extract >> produce >> consume >> transform >> load >> dbt
