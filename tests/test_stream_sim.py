import json
from pathlib import Path

from realtime_monitor.cli import run_microbatch
from realtime_monitor.config import MonitorConfig
from realtime_monitor.quality.incident_store import IncidentStore


def _write_events(path: Path) -> None:
    events = [
        {
            "event_id": "1",
            "timestamp": "2026-03-02T00:00:00Z",
            "source": "web",
            "user_id": "u1",
            "status": "ok",
        },
        {
            "event_id": "1",
            "timestamp": "2026-03-02T00:01:00Z",
            "source": "web",
            "user_id": "u2",
            "status": "error",
        },
        {
            "event_id": "3",
            "timestamp": "2026-03-02T00:02:00Z",
            "source": "web",
            "user_id": "u3",
            "status": "ok",
            "extra": None,
        },
        {
            "event_id": "4",
            "timestamp": "2026-03-02T00:03:00Z",
            "source": "web",
            "user_id": "u4",
            "status": "ok",
        },
        {
            "event_id": "5",
            "timestamp": "2026-03-02T00:04:00Z",
            "source": "web",
            "user_id": "u5",
            "status": "ok",
        },
        {
            "event_id": "6",
            "timestamp": "2026-03-02T00:05:00Z",
            "source": "web",
            "user_id": "u6",
            "status": "ok",
        },
    ]
    path.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")


def test_run_3_microbatches_creates_incidents(tmp_path: Path) -> None:
    input_file = tmp_path / "events.jsonl"
    _write_events(input_file)

    config = MonitorConfig(
        data_dir=tmp_path / "data",
        checkpoint_file=tmp_path / "data" / "checkpoint.json",
        sqlite_path=tmp_path / "data" / "incidents.db",
        batch_size=2,
        min_volume_threshold=2,
        duplicate_ratio_threshold=0.1,
        null_ratio_threshold=0.05,
    )

    run_microbatch(config, input_file)

    store = IncidentStore(config.sqlite_path)
    recent = store.get_recent_incidents(limit=20)
    assert len(recent) >= 2
    raw_count = store.query("SELECT COUNT(*) AS c FROM raw_events")[0]["c"]
    assert raw_count == 6
