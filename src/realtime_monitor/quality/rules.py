from __future__ import annotations

from typing import Any

REQUIRED_FIELDS = {
    "event_id": str,
    "timestamp": str,
    "source": str,
    "user_id": str,
    "status": str,
}
ALLOWED_STATUS = {"ok", "error"}


def validate_event(event: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for key, expected_type in REQUIRED_FIELDS.items():
        if key not in event:
            errors.append(f"missing:{key}")
            continue
        if not isinstance(event[key], expected_type):
            errors.append(f"type:{key}")
    status = event.get("status")
    if isinstance(status, str) and status not in ALLOWED_STATUS:
        errors.append("allowed:status")
    return (len(errors) == 0, errors)
