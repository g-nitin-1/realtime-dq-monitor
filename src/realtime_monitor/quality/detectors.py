from __future__ import annotations

from datetime import datetime, timezone

from realtime_monitor.config import MonitorConfig
from realtime_monitor.metrics.baselines import z_score


def _severity(rule: str) -> str:
    if rule in {"schema_drift", "volume_drop"}:
        return "P0"
    if rule in {"duplicate_surge", "null_spike"}:
        return "P1"
    return "P2"


def detect_incidents(
    batch_metrics: dict[str, float],
    baseline_history: list[dict[str, float]],
    config: MonitorConfig,
    source: str = "all",
    quality_stats: dict[str, float] | None = None,
) -> list[dict[str, str | float]]:
    incidents: list[dict[str, str | float]] = []

    if batch_metrics["volume"] < config.min_volume_threshold:
        incidents.append(
            {
                "rule": "volume_drop",
                "severity": _severity("volume_drop"),
                "source": source,
                "evidence": f"volume={batch_metrics['volume']} threshold={config.min_volume_threshold}",
            }
        )

    if batch_metrics["duplicate_ratio"] > config.duplicate_ratio_threshold:
        incidents.append(
            {
                "rule": "duplicate_surge",
                "severity": _severity("duplicate_surge"),
                "source": source,
                "evidence": (
                    f"duplicate_ratio={batch_metrics['duplicate_ratio']:.3f} "
                    f"threshold={config.duplicate_ratio_threshold}"
                ),
            }
        )

    if batch_metrics["null_ratio"] > config.null_ratio_threshold:
        incidents.append(
            {
                "rule": "null_spike",
                "severity": _severity("null_spike"),
                "source": source,
                "evidence": (
                    f"null_ratio={batch_metrics['null_ratio']:.3f} "
                    f"threshold={config.null_ratio_threshold}"
                ),
            }
        )

    history_volumes = [point["volume"] for point in baseline_history if point.get("volume") is not None]
    z = z_score(batch_metrics["volume"], history_volumes) if history_volumes else 0.0
    if abs(z) >= config.outlier_zscore_threshold:
        incidents.append(
            {
                "rule": "outlier_volume",
                "severity": _severity("outlier_volume"),
                "source": source,
                "evidence": f"volume={batch_metrics['volume']} z_score={z:.2f}",
            }
        )

    if quality_stats is not None:
        schema_drift_ratio = quality_stats.get("schema_drift_ratio", 0.0)
        if schema_drift_ratio > config.schema_drift_threshold:
            incidents.append(
                {
                    "rule": "schema_drift",
                    "severity": _severity("schema_drift"),
                    "source": source,
                    "evidence": (
                        f"schema_drift_ratio={schema_drift_ratio:.3f} "
                        f"threshold={config.schema_drift_threshold}"
                    ),
                }
            )

    now = datetime.now(timezone.utc).isoformat()
    for incident in incidents:
        incident["detected_at"] = now
    return incidents
