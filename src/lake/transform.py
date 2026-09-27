import json
from pathlib import Path

import pandas as pd


RAW_ROOT = Path("data/lake/raw/crypto_market")
PROCESSED_ROOT = Path("data/lake/processed/crypto_market")


def get_raw_events() -> list[dict]:
    """Read all raw cryptocurrency events from the data lake."""

    files = sorted(
        RAW_ROOT.rglob("*.json")
    )

    if not files:
        raise FileNotFoundError(
            "No raw cryptocurrency events found."
        )

    events = []

    for file in files:
        with file.open(
            "r",
            encoding="utf-8",
        ) as f:
            events.append(json.load(f))

    return events


def transform_events(
    events: list[dict],
) -> pd.DataFrame:
    """Flatten Kafka event envelopes into a tabular dataset."""

    records = []

    for event in events:
        data = event["data"]

        records.append(
            {
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "source": event["source"],
                "received_at": event["received_at"],
                "id": data["id"],
                "symbol": data["symbol"],
                "name": data["name"],
                "current_price": data["current_price"],
                "market_cap": data["market_cap"],
                "market_cap_rank": data["market_cap_rank"],
                "total_volume": data["total_volume"],
                "high_24h": data["high_24h"],
                "low_24h": data["low_24h"],
                "price_change_24h": data["price_change_24h"],
                "price_change_percentage_24h": (
                    data["price_change_percentage_24h"]
                ),
                "circulating_supply": data["circulating_supply"],
                "total_supply": data["total_supply"],
                "max_supply": data["max_supply"],
                "ath": data["ath"],
                "atl": data["atl"],
                "last_updated": data["last_updated"],
            }
        )

    df = pd.DataFrame(records)

    df["received_at"] = (
        pd.to_datetime(df["received_at"], utc=True)
        .dt.tz_localize(None)
        .astype("datetime64[us]")
    )

    df["last_updated"] = (
        pd.to_datetime(df["last_updated"], utc=True)
        .dt.tz_localize(None)
        .astype("datetime64[us]")
    )

    return df


def save_processed_data(
    df: pd.DataFrame,
) -> Path:
    """Save transformed data as a Parquet file."""

    now = pd.Timestamp.now(tz="UTC")

    output_dir = (
        PROCESSED_ROOT
        / f"year={now.year}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / f"crypto_market_{now.strftime('%Y%m%dT%H%M%SZ')}.parquet"
    )

    df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False,
    )

    return output_path


def main():
    print("=" * 60)
    print("RAW DATA LAKE → PROCESSED DATA LAKE")
    print("=" * 60)

    events = get_raw_events()

    print(
        f"\nRead {len(events)} raw events"
    )

    df = transform_events(events)

    output_path = save_processed_data(df)

    print(
        f"Transformed {len(df)} cryptocurrency records"
    )

    print(
        f"Saved Parquet file to:\n{output_path}"
    )

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
                "market_cap_rank",
                "total_volume",
                "last_updated",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
