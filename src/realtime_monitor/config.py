from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class MonitorConfig:
    data_dir: Path = Path("data")
    checkpoint_file: Path = Path("data/checkpoint.json")
    sqlite_path: Path = Path("data/incidents.db")
    digest_path: Path = Path("data/daily_digest.md")
    adapter_name: str = "canonical"
    batch_size: int = 50
    sleep_seconds_per_batch: float = 0.0
    max_batches: int | None = None
    resolve_limit: int = 1
    min_volume_threshold: int = 5
    duplicate_ratio_threshold: float = 0.15
    null_ratio_threshold: float = 0.20
    schema_drift_threshold: float = 0.05
    outlier_zscore_threshold: float = 3.0
