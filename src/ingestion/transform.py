import json
from pathlib import Path

import pandas as pd


def get_latest_raw_file() -> Path:
    """Find the most recently created raw JSON file."""

    raw_files = sorted(
        Path("data/raw").glob("crypto_market_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not raw_files:
        raise FileNotFoundError(
            "No raw cryptocurrency JSON files found."
        )

    return raw_files[0]


def transform_raw_data(raw_file: Path) -> pd.DataFrame:
    """Read and transform the latest raw JSON file."""

    with raw_file.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    metadata = payload["metadata"]
    data = payload["data"]

    df = pd.DataFrame(data)

    # Add pipeline ingestion timestamp.
    df["ingested_at"] = metadata["ingested_at"]

    # Convert timestamps to UTC datetime values.
    df["last_updated"] = pd.to_datetime(
        df["last_updated"],
        utc=True,
    )

    df["ingested_at"] = pd.to_datetime(
        df["ingested_at"],
        utc=True,
    )

    # Flatten the nested ROI object.
    if "roi" in df.columns:
        df["roi_times"] = df["roi"].apply(
            lambda value: value.get("times")
            if isinstance(value, dict)
            else None
        )

        df["roi_currency"] = df["roi"].apply(
            lambda value: value.get("currency")
            if isinstance(value, dict)
            else None
        )

        df["roi_percentage"] = df["roi"].apply(
            lambda value: value.get("percentage")
            if isinstance(value, dict)
            else None
        )

        # Remove the original nested object.
        df = df.drop(columns=["roi"])

    return df


def save_processed_data(df: pd.DataFrame) -> Path:
    """Save transformed cryptocurrency data as Parquet."""

    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = pd.Timestamp.now(tz="UTC")

    filename = timestamp.strftime(
        "crypto_market_%Y%m%d_%H%M%S.parquet"
    )

    output_path = output_dir / filename

    df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False,
    )

    return output_path


def main():
    raw_file = get_latest_raw_file()

    print(f"Reading raw file: {raw_file}")

    df = transform_raw_data(raw_file)

    output_path = save_processed_data(df)

    print(f"Transformed {len(df)} cryptocurrency records")
    print(f"Processed data saved to: {output_path}")

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nData:")
    print(
        df[
            [
                "id",
                "symbol",
                "name",
                "current_price",
                "market_cap",
                "total_volume",
                "roi_times",
                "roi_currency",
                "roi_percentage",
                "last_updated",
                "ingested_at",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()