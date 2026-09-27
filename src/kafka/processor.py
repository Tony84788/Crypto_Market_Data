import logging


logger = logging.getLogger(__name__)


REQUIRED_FIELDS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "last_updated",
]


def validate_crypto_event(event: dict) -> list[str]:
    """Validate the required fields in a cryptocurrency event."""

    errors = []

    if not isinstance(event, dict):
        errors.append("Event must be a dictionary.")
        return errors

    for field in REQUIRED_FIELDS:
        if field not in event:
            errors.append(
                f"Missing required field: {field}"
            )

    if "current_price" in event:
        if not isinstance(
            event["current_price"],
            (int, float),
        ):
            errors.append(
                "current_price must be numeric."
            )

    return errors


def process_crypto_event(event: dict) -> dict | None:
    """
    Validate and process a cryptocurrency Kafka event.

    Returns a simplified event when valid.
    Returns None when invalid.
    """

    errors = validate_crypto_event(event)

    if errors:
        logger.error(
            "Invalid cryptocurrency event: %s",
            errors,
        )
        return None

    processed_event = {
        "id": event["id"],
        "symbol": event["symbol"],
        "name": event["name"],
        "current_price": event["current_price"],
        "market_cap": event.get("market_cap"),
        "total_volume": event.get("total_volume"),
        "market_cap_rank": event.get("market_cap_rank"),
        "last_updated": event["last_updated"],
    }

    logger.info(
        "Processed crypto event: %s (%s)",
        processed_event["name"],
        processed_event["symbol"],
    )

    return processed_event
