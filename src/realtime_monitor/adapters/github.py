from __future__ import annotations

from typing import Any


def _as_str(value: Any, default: str) -> str:
    if value is None:
        return default
    return str(value)


def adapt(event: dict[str, Any]) -> dict[str, Any]:
    """Map GitHub event-style payloads to canonical schema."""
    actor = event.get("actor")
    repo = event.get("repo")
    org = event.get("org")

    source = "unknown_source"
    if isinstance(repo, dict) and repo.get("name"):
        source = _as_str(repo.get("name"), source)
    elif isinstance(org, dict) and org.get("login"):
        source = _as_str(org.get("login"), source)

    user_id = "unknown_user"
    if isinstance(actor, dict) and actor.get("login"):
        user_id = _as_str(actor.get("login"), user_id)

    ts = event.get("created_at")
    timestamp = _as_str(ts, "")

    event_id = _as_str(event.get("id"), "")
    event_type = _as_str(event.get("type"), "")

    status = "ok"
    if not timestamp or not event_id:
        status = "error"
    elif event_type.lower().endswith("errorevent"):
        status = "error"

    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "source": source,
        "user_id": user_id,
        "status": status,
    }
