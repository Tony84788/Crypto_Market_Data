from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REQUIRED_COLUMNS = {
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "market_cap_rank",
    "total_volume",
    "last_updated",
    "ingested_at",
}


def get_latest_parquet_file() -> Path:
    """Find the most recently created Parquet file."""

    parquet_files = sorted(
        Path("data/processed").glob("crypto_market_*.parquet"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not parquet_files:
        raise FileNotFoundError(
            "No processed Parquet files found."
        )

    return parquet_files[0]


def validate_schema(df: pd.DataFrame) -> list[str]:
    """Validate that all required columns exist."""

    errors = []

    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        errors.append(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    return errors


def validate_required_values(df: pd.DataFrame) -> list[str]:
    """Validate that required columns contain no null values."""

    errors = []

    required_columns = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "last_updated",
        "ingested_at",
    ]

    for column in required_columns:
        null_count = df[column].isna().sum()

        if null_count > 0:
            errors.append(
                f"{column} contains {null_count} null values"
            )

    return errors


def validate_numeric_ranges(df: pd.DataFrame) -> list[str]:
    """Validate that important numeric values are non-negative."""

    errors = []

    numeric_columns = [
        "current_price",
        "market_cap",
        "total_volume",
    ]

    for column in numeric_columns:
        invalid_count = (df[column] < 0).sum()

        if invalid_count > 0:
            errors.append(
                f"{column} contains {invalid_count} negative values"
            )

    return errors


def validate_unique_ids(df: pd.DataFrame) -> list[str]:
    """Validate that cryptocurrency IDs are unique."""

    errors = []

    duplicate_count = df["id"].duplicated().sum()

    if duplicate_count > 0:
        errors.append(
            f"Found {duplicate_count} duplicate cryptocurrency IDs"
        )

    return errors


def validate_timestamps(df: pd.DataFrame) -> list[str]:
    """Validate timestamp columns."""

    errors = []

    timestamp_columns = [
        "last_updated",
        "ingested_at",
    ]

    for column in timestamp_columns:
        if not pd.api.types.is_datetime64_any_dtype(df[column]):
            errors.append(
                f"{column} is not a datetime column"
            )

    return errors


def validate_timestamp_freshness(
    df: pd.DataFrame,
    max_age_minutes: int = 30,
) -> list[str]:
    """Validate that market data was fresh when it was ingested."""

    errors = []

    data_age = df["ingested_at"] - df["last_updated"]

    stale_records = data_age > pd.Timedelta(max_age_minutes, unit="m")

    stale_count = stale_records.sum()

    if stale_count > 0:
        errors.append(
            f"{stale_count} records were already older than "
            f"{max_age_minutes} minutes when ingested"
        )

    return errors


def validate_data(df: pd.DataFrame) -> list[str]:
    """Run all data-quality checks."""

    errors = []

    errors.extend(validate_schema(df))
    errors.extend(validate_required_values(df))
    errors.extend(validate_numeric_ranges(df))
    errors.extend(validate_unique_ids(df))
    errors.extend(validate_timestamps(df))
    errors.extend(validate_timestamp_freshness(df))

    return errors


def main():
    parquet_file = get_latest_parquet_file()

    print("=" * 60)
    print("DATA QUALITY VALIDATION")
    print("=" * 60)

    print(f"\nFile: {parquet_file}")

    df = pd.read_parquet(parquet_file)

    errors = validate_data(df)

    print("\nChecks:")
    print("✓ Schema validation")
    print("✓ Required-value validation")
    print("✓ Numeric-range validation")
    print("✓ Duplicate-ID validation")
    print("✓ Timestamp validation")
    print("✓ Timestamp freshness validation")

    print("\n" + "=" * 60)

    if errors:
        print("VALIDATION FAILED")
        print("=" * 60)

        for error in errors:
            print(f"✗ {error}")

        raise SystemExit(1)

    print("VALIDATION PASSED")
    print("=" * 60)
    print("All data-quality checks passed.")


if __name__ == "__main__":
    main()
