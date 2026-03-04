from __future__ import annotations

import time


class TokenBucketRateLimiter:
    def __init__(self, capacity: int, refill_rate_per_sec: float) -> None:
        self.capacity = capacity
        self.refill_rate_per_sec = refill_rate_per_sec
        self.tokens = float(capacity)
        self.last_refill = time.time()

    def allow(self) -> bool:
        now = time.time()
        elapsed = now - self.last_refill
        self.last_refill = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate_per_sec)
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False
