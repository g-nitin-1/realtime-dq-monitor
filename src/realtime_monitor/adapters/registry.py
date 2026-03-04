from __future__ import annotations

from typing import Any

from realtime_monitor.adapters import canonical, github

ADAPTERS: dict[str, Any] = {
    "canonical": canonical.adapt,
    "github": github.adapt,
}


def adapt_event(event: dict[str, Any], adapter_name: str) -> dict[str, Any]:
    adapter = ADAPTERS.get(adapter_name)
    if adapter is None:
        raise ValueError(f"Unknown adapter: {adapter_name}")
    return adapter(event)
