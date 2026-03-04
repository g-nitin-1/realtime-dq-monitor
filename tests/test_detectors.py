from realtime_monitor.config import MonitorConfig
from realtime_monitor.quality.detectors import detect_incidents


def test_detect_incidents_thresholds() -> None:
    cfg = MonitorConfig(min_volume_threshold=5, duplicate_ratio_threshold=0.1, null_ratio_threshold=0.1)
    batch_metrics = {
        "volume": 2.0,
        "unique_users": 2.0,
        "error_rate": 0.0,
        "duplicate_ratio": 0.2,
        "null_ratio": 0.15,
    }

    incidents = detect_incidents(batch_metrics, baseline_history=[], config=cfg)
    rules = {item["rule"] for item in incidents}
    assert "volume_drop" in rules
    assert "duplicate_surge" in rules
    assert "null_spike" in rules


def test_detect_schema_drift_incident() -> None:
    cfg = MonitorConfig(schema_drift_threshold=0.1)
    batch_metrics = {
        "volume": 50.0,
        "unique_users": 45.0,
        "error_rate": 0.02,
        "duplicate_ratio": 0.0,
        "null_ratio": 0.0,
    }
    incidents = detect_incidents(
        batch_metrics,
        baseline_history=[],
        config=cfg,
        quality_stats={"schema_drift_ratio": 0.2},
    )
    assert any(item["rule"] == "schema_drift" for item in incidents)
