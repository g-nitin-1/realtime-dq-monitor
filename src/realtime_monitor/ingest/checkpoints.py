from __future__ import annotations

import json
from pathlib import Path


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load_offset(self) -> int:
        if not self.path.exists():
            return 0
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return int(data.get("offset", 0))

    def save_offset(self, offset: int) -> None:
        self.path.write_text(json.dumps({"offset": offset}), encoding="utf-8")
