import json
from datetime import datetime, timezone
from pathlib import Path


LAKE_ROOT = Path("data/lake/raw/crypto_market")


def save_raw_event(event: dict) -> Path:
    """
    Save a raw Kafka event to the local data lake.

    Data is partitioned by ingestion date:
    year=YYYY/month=MM/day=DD
    """

    now = datetime.now(timezone.utc)

    partition_dir = (
        LAKE_ROOT
        / f"year={now.year}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
    )

    partition_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    event_id = event.get("event_id", "unknown")

    timestamp = now.strftime("%Y%m%dT%H%M%S%fZ")

    output_path = (
        partition_dir
        / f"{event_id}_{timestamp}.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            event,
            file,
            indent=2,
            ensure_ascii=False,
        )

    return output_path
