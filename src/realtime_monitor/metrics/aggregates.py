from __future__ import annotations

from collections import Counter
from typing import Any


def compute_batch_metrics(events: list[dict[str, Any]]) -> dict[str, float]:
    if not events:
        return {
            "volume": 0,
            "unique_users": 0,
            "error_rate": 0.0,
            "duplicate_ratio": 0.0,
            "null_ratio": 0.0,
        }

    volume = len(events)
    unique_users = len({event["user_id"] for event in events})
    error_count = sum(1 for event in events if event.get("status") == "error")

    ids = [event["event_id"] for event in events]
    counter = Counter(ids)
    duplicate_count = sum(count - 1 for count in counter.values() if count > 1)

    total_fields = 0
    null_fields = 0
    for event in events:
        total_fields += len(event)
        null_fields += sum(1 for value in event.values() if value in (None, ""))

    return {
        "volume": float(volume),
        "unique_users": float(unique_users),
        "error_rate": error_count / volume,
        "duplicate_ratio": duplicate_count / volume,
        "null_ratio": (null_fields / total_fields) if total_fields else 0.0,
    }
