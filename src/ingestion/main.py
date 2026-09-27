import json
from datetime import datetime, timezone
from pathlib import Path

from src.config.settings import settings
from src.ingestion.coingecko_client import CoinGeckoClient


def save_raw_data(data: list[dict]) -> Path:
    """Save the raw CoinGecko response together with ingestion metadata."""

    ingested_at = datetime.now(timezone.utc)

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = ingested_at.strftime("crypto_market_%Y%m%d_%H%M%S.json")
    output_path = output_dir / filename

    raw_payload = {
        "metadata": {
            "source": "CoinGecko API",
            "base_url": settings.coingecko_base_url,
            "endpoint": "/coins/markets",
            "ingested_at": ingested_at.isoformat(),
            "record_count": len(data),
        },
        "data": data,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, indent=2)

    return output_path


def main():
    client = CoinGeckoClient()

    market_data = client.get_market_data(
        coin_ids=[
            "bitcoin",
            "ethereum",
            "solana",
        ]
    )

    output_path = save_raw_data(market_data)

    print(f"Retrieved {len(market_data)} cryptocurrencies")
    print(f"Raw data saved to: {output_path}")


if __name__ == "__main__":
    main()