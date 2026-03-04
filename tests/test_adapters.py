from pathlib import Path

from realtime_monitor.adapters.github import adapt as github_adapt
from realtime_monitor.cli import run_microbatch
from realtime_monitor.config import MonitorConfig
from realtime_monitor.quality.incident_store import IncidentStore


def test_github_adapter_maps_fields() -> None:
    raw = {
        "id": "2489564054",
        "type": "WatchEvent",
        "actor": {"login": "manishsethi"},
        "repo": {"name": "opscode-cookbooks/aws"},
        "created_at": "2015-01-01T11:00:00Z",
    }
    event = github_adapt(raw)
    assert event["event_id"] == "2489564054"
    assert event["timestamp"] == "2015-01-01T11:00:00Z"
    assert event["source"] == "opscode-cookbooks/aws"
    assert event["user_id"] == "manishsethi"
    assert event["status"] == "ok"


def test_run_microbatch_with_github_adapter(tmp_path: Path) -> None:
    input_file = tmp_path / "github.jsonl"
    input_file.write_text(
        "\n".join(
            [
                '{"id":"1","type":"WatchEvent","actor":{"login":"u1"},"repo":{"name":"r1"},"created_at":"2015-01-01T11:00:00Z"}',
                '{"id":"2","type":"WatchEvent","actor":{"login":"u2"},"repo":{"name":"r1"},"created_at":"2015-01-01T11:01:00Z"}',
            ]
        ),
        encoding="utf-8",
    )

    cfg = MonitorConfig(
        data_dir=tmp_path / "data",
        checkpoint_file=tmp_path / "data" / "checkpoint.json",
        sqlite_path=tmp_path / "data" / "incidents.db",
        adapter_name="github",
        batch_size=10,
        min_volume_threshold=1,
    )
    run_microbatch(cfg, input_file)

    store = IncidentStore(cfg.sqlite_path)
    silver_valid = store.query("SELECT COUNT(*) AS c FROM silver_events WHERE is_valid = 1")[0]["c"]
    raw_count = store.query("SELECT COUNT(*) AS c FROM raw_events")[0]["c"]
    assert silver_valid == 2
    assert raw_count == 2
