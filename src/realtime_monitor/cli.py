from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from realtime_monitor.adapters.registry import ADAPTERS, adapt_event
from realtime_monitor.config import MonitorConfig
from realtime_monitor.ingest.checkpoints import CheckpointStore
from realtime_monitor.ingest.source import EventSource
from realtime_monitor.metrics.aggregates import compute_batch_metrics
from realtime_monitor.metrics.digest import generate_daily_digest
from realtime_monitor.quality.alerting import AlertAdapter, ConsoleAlerter
from realtime_monitor.quality.detectors import detect_incidents
from realtime_monitor.quality.incident_store import IncidentStore
from realtime_monitor.quality.rules import validate_event
from realtime_monitor.quality.sql_queries import (
    anomaly_summary,
    rolling_window_metrics,
    rolling_window_metrics_5m_1h_1d,
    sla_time_to_detect_resolve,
)
from realtime_monitor.utils.heavy_hitters import HeavyHitters
from realtime_monitor.utils.logging import get_logger
from realtime_monitor.utils.rate_limit import TokenBucketRateLimiter


def _quality_stats(quarantine: list[dict[str, object]], batch_size: int) -> dict[str, float]:
    if batch_size <= 0:
        return {"schema_drift_ratio": 0.0}

    schema_error_events = 0
    for row in quarantine:
        errors = row.get("errors", [])
        if not isinstance(errors, list):
            continue
        if any(str(err).startswith("missing:") or str(err).startswith("type:") for err in errors):
            schema_error_events += 1

    return {"schema_drift_ratio": schema_error_events / batch_size}


def run_microbatch(config: MonitorConfig, input_path: Path) -> None:
    logger = get_logger()
    source = EventSource(input_path)
    checkpoint = CheckpointStore(config.checkpoint_file)
    incident_store = IncidentStore(config.sqlite_path)
    alerter = ConsoleAlerter(TokenBucketRateLimiter(capacity=10, refill_rate_per_sec=1.0))
    adapter = AlertAdapter()
    heavy_hitters = HeavyHitters()

    events = source.read_all()
    offset = checkpoint.load_offset()
    new_events = events[offset:]
    batches = EventSource.chunk(new_events, config.batch_size)
    if config.max_batches is not None:
        batches = batches[: config.max_batches]

    baseline_history: list[dict[str, float]] = []
    processed_count = 0

    for batch_idx, batch in enumerate(batches, start=1):
        incident_store.add_raw_events(batch_idx, config.adapter_name, batch)

        valid_events: list[dict[str, object]] = []
        quarantine: list[dict[str, object]] = []
        for raw_event in batch:
            event = adapt_event(raw_event, config.adapter_name)
            valid, errors = validate_event(event)
            if valid:
                valid_events.append(event)
            else:
                quarantine.append({"event": event, "errors": errors, "raw_event": raw_event})

        incident_store.add_silver_events(batch_idx, valid_events, quarantine)
        metrics = compute_batch_metrics(valid_events)
        incident_store.add_rolling_metrics(batch_idx, metrics)
        quality_stats = _quality_stats(quarantine, len(batch))

        first_event_ts = None
        if valid_events:
            first_event_ts = str(valid_events[0].get("timestamp", ""))
        first_seen_at = datetime.now(timezone.utc).isoformat()

        incidents = detect_incidents(
            metrics,
            baseline_history,
            config,
            quality_stats=quality_stats,
        )
        baseline_history.append(metrics)

        for incident in incidents:
            incident["evidence_map"] = {
                "batch_id": str(batch_idx),
                "volume": str(metrics["volume"]),
                "duplicate_ratio": f"{metrics['duplicate_ratio']:.6f}",
                "null_ratio": f"{metrics['null_ratio']:.6f}",
                "schema_drift_ratio": f"{quality_stats['schema_drift_ratio']:.6f}",
                "first_seen_at": first_seen_at,
            }
            if first_event_ts:
                incident["evidence_map"]["first_event_timestamp"] = first_event_ts

            incident_id = incident_store.add_incident(incident)
            heavy_hitters.add(str(incident["rule"]))
            message = (
                f"{incident['severity']}: {incident['rule']} detected "
                f"(source={incident['source']}, incident_id={incident_id})"
            )
            if alerter.alert(message):
                adapter.send({"text": message})

        processed_count += len(batch)
        logger.info(
            "Batch %s processed %s events; %s incidents detected; %s quarantined",
            batch_idx,
            len(batch),
            len(incidents),
            len(quarantine),
        )

        if config.sleep_seconds_per_batch > 0:
            time.sleep(config.sleep_seconds_per_batch)

    resolved_count = incident_store.simulate_resolves(max_resolves=config.resolve_limit)
    checkpoint.save_offset(offset + processed_count)

    conn = sqlite3.connect(config.sqlite_path)
    conn.row_factory = sqlite3.Row
    rolling = rolling_window_metrics(conn, window_size=3)
    anomaly = anomaly_summary(conn)
    sla = sla_time_to_detect_resolve(conn)
    conn.close()

    digest_path = generate_daily_digest(config.sqlite_path, config.digest_path)

    logger.info("Top incident rules: %s", heavy_hitters.top_k(3))
    logger.info("Resolved incidents in simulation: %s", resolved_count)
    logger.info("Rolling metrics rows: %s", len(rolling))
    logger.info("Anomaly summary rows: %s", len(anomaly))
    logger.info("SLA rows: %s", len(sla))
    logger.info("Digest generated at: %s", digest_path)


def generate_report(config: MonitorConfig, report_type: str, window_size: int = 3) -> list[dict[str, Any]]:
    conn = sqlite3.connect(config.sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        if report_type == "rolling":
            return rolling_window_metrics(conn, window_size=window_size)
        if report_type == "anomaly":
            return anomaly_summary(conn)
        if report_type == "sla":
            return sla_time_to_detect_resolve(conn)
        if report_type == "windows":
            return rolling_window_metrics_5m_1h_1d(conn)
        raise ValueError(f"Unsupported report type: {report_type}")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Realtime DQ monitor CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run micro-batch ingest and detection")
    run_parser.add_argument("--input", type=Path, required=True, help="Path to newline-delimited JSON events")
    run_parser.add_argument(
        "--adapter",
        choices=sorted(ADAPTERS.keys()),
        default="canonical",
        help="Adapter used to map raw events into canonical schema",
    )
    run_parser.add_argument("--reset-state", action="store_true", help="Delete checkpoint/database before run")
    run_parser.add_argument("--batch-size", type=int, default=50)
    run_parser.add_argument("--max-batches", type=int, default=None)
    run_parser.add_argument("--sleep-seconds", type=float, default=0.0)
    run_parser.add_argument("--resolve-limit", type=int, default=1)
    run_parser.add_argument("--min-volume-threshold", type=int, default=5)
    run_parser.add_argument("--duplicate-ratio-threshold", type=float, default=0.15)
    run_parser.add_argument("--null-ratio-threshold", type=float, default=0.20)
    run_parser.add_argument("--schema-drift-threshold", type=float, default=0.05)
    run_parser.add_argument("--outlier-zscore-threshold", type=float, default=3.0)

    report_parser = subparsers.add_parser("report", help="Print analytics report from SQLite")
    report_parser.add_argument(
        "--type",
        choices=["rolling", "anomaly", "sla", "windows"],
        required=True,
        help="Report type",
    )
    report_parser.add_argument(
        "--window-size",
        type=int,
        default=3,
        help="Window size for rolling report",
    )
    report_parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Optional SQLite path override",
    )

    args = parser.parse_args()

    config = MonitorConfig()
    if getattr(args, "db_path", None) is not None:
        config.sqlite_path = args.db_path
    config.data_dir.mkdir(parents=True, exist_ok=True)

    if args.command == "run":
        config.batch_size = args.batch_size
        config.adapter_name = args.adapter
        config.max_batches = args.max_batches
        config.sleep_seconds_per_batch = args.sleep_seconds
        config.resolve_limit = args.resolve_limit
        config.min_volume_threshold = args.min_volume_threshold
        config.duplicate_ratio_threshold = args.duplicate_ratio_threshold
        config.null_ratio_threshold = args.null_ratio_threshold
        config.schema_drift_threshold = args.schema_drift_threshold
        config.outlier_zscore_threshold = args.outlier_zscore_threshold

        if args.reset_state:
            for path in [config.checkpoint_file, config.sqlite_path, config.digest_path]:
                if path.exists():
                    path.unlink()

        run_microbatch(config, args.input)
        return

    rows = generate_report(config, args.type, window_size=args.window_size)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
