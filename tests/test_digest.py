from pathlib import Path

from realtime_monitor.metrics.digest import generate_daily_digest
from realtime_monitor.quality.incident_store import IncidentStore


def test_generate_daily_digest(tmp_path: Path) -> None:
    db_path = tmp_path / "incidents.db"
    out_path = tmp_path / "daily_digest.md"

    store = IncidentStore(db_path)
    incident_id = store.add_incident(
        {
            "detected_at": "2026-03-02T00:00:00+00:00",
            "rule": "duplicate_surge",
            "severity": "P1",
            "source": "web",
            "evidence": "duplicate_ratio=0.4 threshold=0.1",
            "evidence_map": {"batch_id": "1"},
        }
    )
    assert incident_id > 0

    result = generate_daily_digest(db_path, out_path)
    text = result.read_text(encoding="utf-8")
    assert "# Daily Digest" in text
    assert "Top Rules" in text
