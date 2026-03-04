from pathlib import Path

from realtime_monitor.cli import generate_report, run_microbatch
from realtime_monitor.config import MonitorConfig


def _seed_events(path: Path) -> None:
    data = [
        '{"event_id":"b1","timestamp":"2026-03-02T00:00:00Z","source":"web","user_id":"u1","status":"ok"}',
        '{"event_id":"b1","timestamp":"2026-03-02T00:00:10Z","source":"web","user_id":"u2","status":"ok"}',
        '{"event_id":"b3","timestamp":"2026-03-02T00:00:20Z","source":"web","user_id":"u3","status":"error"}',
        '{"event_id":"b4","timestamp":"2026-03-02T00:00:30Z","source":"mobile","user_id":"u4","status":"ok"}',
    ]
    path.write_text("\n".join(data), encoding="utf-8")


def test_generate_report_types(tmp_path: Path) -> None:
    input_file = tmp_path / "events.jsonl"
    _seed_events(input_file)

    cfg = MonitorConfig(
        data_dir=tmp_path / "data",
        checkpoint_file=tmp_path / "data/checkpoint.json",
        sqlite_path=tmp_path / "data/incidents.db",
        digest_path=tmp_path / "data/digest.md",
        batch_size=2,
        min_volume_threshold=3,
        duplicate_ratio_threshold=0.1,
    )
    run_microbatch(cfg, input_file)

    assert len(generate_report(cfg, "rolling", window_size=2)) >= 1
    assert len(generate_report(cfg, "anomaly")) >= 1
    assert len(generate_report(cfg, "sla")) >= 1
    assert len(generate_report(cfg, "windows")) >= 1
