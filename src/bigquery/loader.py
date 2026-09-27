from pathlib import Path

from google.cloud import bigquery


PROJECT_ID = "crypto-market-data-platform"
DATASET_ID = "crypto_market"
TABLE_ID = "crypto_prices"

TABLE_REFERENCE = (
    f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
)


def create_bigquery_client() -> bigquery.Client:
    """Create and return a BigQuery client."""
    return bigquery.Client(
        project=PROJECT_ID
    )


def load_parquet_to_bigquery(
    parquet_path: str | Path,
) -> None:
    """
    Load a local Parquet file into BigQuery.
    """

    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}"
        )

    client = create_bigquery_client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_APPEND
        ),
    )

    print(
        f"Loading Parquet file: {parquet_path}"
    )

    with parquet_path.open("rb") as parquet_file:

        load_job = client.load_table_from_file(
            parquet_file,
            TABLE_REFERENCE,
            job_config=job_config,
        )

    load_job.result()

    print(
        f"Successfully loaded data into "
        f"{TABLE_REFERENCE}"
    )


if __name__ == "__main__":
    print(
        "BigQuery loader module ready."
    )