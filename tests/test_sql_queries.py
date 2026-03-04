import sqlite3
from pathlib import Path

from realtime_monitor.cli import run_microbatch
from realtime_monitor.config import MonitorConfig
from realtime_monitor.quality.sql_queries import (
    anomaly_summary,
    rolling_window_metrics,
    rolling_window_metrics_5m_1h_1d,
    sla_time_to_detect_resolve,
)


def _seed_events(path: Path) -> None:
    data = [
        '{"event_id":"a1","timestamp":"2026-03-02T00:00:00Z","source":"web","user_id":"u1","status":"ok"}',
        '{"event_id":"a1","timestamp":"2026-03-02T00:00:10Z","source":"web","user_id":"u2","status":"ok"}',
        '{"event_id":"a3","timestamp":"2026-03-02T00:00:20Z","source":"mobile","user_id":"u3","status":"error"}',
        '{"event_id":"a4","timestamp":"2026-03-02T00:00:30Z","source":"mobile","user_id":"u4","status":"ok"}',
        '{"event_id":"a5","timestamp":"2026-03-02T00:00:40Z","source":"web","user_id":"u5","status":"ok"}',
        '{"event_id":"a6","timestamp":"2026-03-02T00:00:50Z","source":"web","user_id":"u6","status":"ok"}',
    ]
    path.write_text("\n".join(data), encoding="utf-8")


def test_advanced_sql_queries(tmp_path: Path) -> None:
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

    conn = sqlite3.connect(cfg.sqlite_path)
    conn.row_factory = sqlite3.Row

    rolling = rolling_window_metrics(conn, window_size=2)
    assert len(rolling) >= 3

    anomaly = anomaly_summary(conn)
    assert len(anomaly) >= 1

    windows = rolling_window_metrics_5m_1h_1d(conn)
    assert len(windows) >= 1

    sla = sla_time_to_detect_resolve(conn)
    assert len(sla) >= 1

    conn.close()
