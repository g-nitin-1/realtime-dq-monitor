from __future__ import annotations

from typing import Any


def adapt(event: dict[str, Any]) -> dict[str, Any]:
    """Canonical adapter: assumes input already matches internal schema."""
    return event
