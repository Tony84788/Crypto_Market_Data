import pandas as pd

from src.ingestion.validate import (
    validate_schema,
    validate_required_values,
    validate_numeric_ranges,
    validate_unique_ids,
    validate_timestamps,
    validate_timestamp_freshness,
    validate_data,
)


def valid_dataframe():
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)

    return pd.DataFrame(
        {
            "id": ["bitcoin", "ethereum"],
            "symbol": ["btc", "eth"],
            "name": ["Bitcoin", "Ethereum"],
            "current_price": [100000.0, 4000.0],
            "market_cap": [2000000000000, 500000000000],
            "market_cap_rank": [1, 2],
            "total_volume": [50000000000, 20000000000],
            "last_updated": [
                now - pd.Timedelta(5, unit="m"),
                now - pd.Timedelta(10, unit="m"),
            ],
            "ingested_at": [now, now],
        }
    )


def test_validate_schema_passes_for_valid_dataframe():
    df = valid_dataframe()

    assert validate_schema(df) == []


def test_validate_schema_detects_missing_columns():
    df = valid_dataframe().drop(columns=["market_cap"])

    errors = validate_schema(df)

    assert len(errors) == 1
    assert "market_cap" in errors[0]


def test_validate_required_values_detects_nulls():
    df = valid_dataframe()
    df.loc[0, "current_price"] = None

    errors = validate_required_values(df)

    assert len(errors) == 1
    assert "current_price contains 1 null values" in errors[0]


def test_validate_numeric_ranges_detects_negative_values():
    df = valid_dataframe()
    df.loc[0, "current_price"] = -100

    errors = validate_numeric_ranges(df)

    assert len(errors) == 1
    assert "current_price contains 1 negative values" in errors[0]


def test_validate_unique_ids_detects_duplicates():
    df = valid_dataframe()
    df.loc[1, "id"] = "bitcoin"

    errors = validate_unique_ids(df)

    assert len(errors) == 1
    assert "duplicate cryptocurrency IDs" in errors[0]


def test_validate_timestamps_passes_for_datetime_columns():
    df = valid_dataframe()

    assert validate_timestamps(df) == []


def test_validate_timestamps_detects_invalid_timestamp_type():
    df = valid_dataframe()
    df["last_updated"] = ["not-a-date", "not-a-date"]

    errors = validate_timestamps(df)

    assert len(errors) == 1
    assert "last_updated is not a datetime column" in errors[0]


def test_validate_timestamp_freshness_detects_stale_data():
    df = valid_dataframe()

    df.loc[0, "last_updated"] = (
        df.loc[0, "ingested_at"] - pd.Timedelta(60, unit="m")
    )

    errors = validate_timestamp_freshness(df, max_age_minutes=30)

    assert len(errors) == 1
    assert "1 records were already older than 30 minutes" in errors[0]


def test_validate_data_passes_for_valid_dataframe():
    df = valid_dataframe()

    assert validate_data(df) == []
