from __future__ import annotations

from collections import Counter


class HeavyHitters:
    def __init__(self) -> None:
        self.counter: Counter[str] = Counter()

    def add(self, key: str, weight: int = 1) -> None:
        self.counter[key] += weight

    def top_k(self, k: int) -> list[tuple[str, int]]:
        return self.counter.most_common(k)
