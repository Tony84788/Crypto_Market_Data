from pathlib import Path

import pandas as pd


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


def inspect_parquet(file_path: Path) -> None:
    """Inspect the processed cryptocurrency dataset."""

    df = pd.read_parquet(file_path)

    print("=" * 60)
    print("PARQUET INSPECTION")
    print("=" * 60)

    print(f"\nFile: {file_path}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    print("\n" + "=" * 60)
    print("DATA TYPES")
    print("=" * 60)

    print(df.dtypes)

    print("\n" + "=" * 60)
    print("NULL VALUES")
    print("=" * 60)

    null_counts = df.isnull().sum()

    print(null_counts)

    print("\n" + "=" * 60)
    print("DUPLICATE ROWS")
    print("=" * 60)

    duplicate_count = df["id"].duplicated().sum()

    print(f"Duplicate cryptocurrency IDs: {duplicate_count}")

    print("\n" + "=" * 60)
    print("CRYPTOCURRENCY IDs")
    print("=" * 60)

    print(df["id"].tolist())

    print("\n" + "=" * 60)
    print("DATA PREVIEW")
    print("=" * 60)

    print(
        df[
            [
                "id",
                "symbol",
                "name",
                "current_price",
                "market_cap",
                "total_volume",
            ]
        ].to_string(index=False)
    )


def main():
    parquet_file = get_latest_parquet_file()

    inspect_parquet(parquet_file)


if __name__ == "__main__":
    main()