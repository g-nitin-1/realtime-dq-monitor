"""Event adapters for mapping external payloads into canonical monitor schema."""

from realtime_monitor.adapters.registry import ADAPTERS, adapt_event

__all__ = ["ADAPTERS", "adapt_event"]
