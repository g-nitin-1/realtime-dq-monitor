from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class EventSource:
    def __init__(self, path: Path) -> None:
        self.path = path

    def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        lines = [line.strip() for line in self.path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [json.loads(line) for line in lines]

    @staticmethod
    def chunk(events: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
        return [events[i : i + size] for i in range(0, len(events), size)]
