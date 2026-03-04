from __future__ import annotations

from realtime_monitor.utils.rate_limit import TokenBucketRateLimiter


class ConsoleAlerter:
    def __init__(self, rate_limiter: TokenBucketRateLimiter) -> None:
        self.rate_limiter = rate_limiter

    def alert(self, message: str) -> bool:
        if not self.rate_limiter.allow():
            return False
        print(message)
        return True


class AlertAdapter:
    """Placeholder adapter to represent non-console integration (Slack/webhook/email)."""

    def send(self, payload: dict[str, str]) -> None:
        # Intentionally a no-op for prototype safety and test determinism.
        _ = payload
