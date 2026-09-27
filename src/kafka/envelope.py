from datetime import datetime, timezone


def create_event_envelope(
    data: dict,
    event_type: str = "crypto_market_update",
    source: str = "coingecko",
) -> dict:
    """Wrap cryptocurrency data in a standard event envelope."""

    return {
        "event_id": data["id"],
        "event_type": event_type,
        "source": source,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
