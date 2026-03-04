from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


class IncidentStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_events (
                    raw_event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    adapter TEXT NOT NULL,
                    raw_event TEXT NOT NULL,
                    ingested_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS silver_events (
                    event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    event_id TEXT,
                    timestamp TEXT,
                    source TEXT,
                    user_id TEXT,
                    status TEXT,
                    is_valid INTEGER NOT NULL,
                    validation_errors TEXT,
                    raw_event TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rolling_metrics (
                    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    computed_at TEXT NOT NULL,
                    batch_id INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    volume REAL NOT NULL,
                    unique_users REAL NOT NULL,
                    error_rate REAL NOT NULL,
                    duplicate_ratio REAL NOT NULL,
                    null_ratio REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    detected_at TEXT NOT NULL,
                    resolved_at TEXT,
                    rule TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    source TEXT NOT NULL,
                    evidence TEXT NOT NULL,
                    recommendation TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS incident_evidence (
                    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL,
                    evidence_key TEXT NOT NULL,
                    evidence_value TEXT NOT NULL,
                    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id)
                )
                """
            )

    def add_raw_events(self, batch_id: int, adapter_name: str, raw_events: list[dict[str, Any]]) -> None:
        with self._connect() as conn:
            now = datetime.now(timezone.utc).isoformat()
            for event in raw_events:
                conn.execute(
                    """
                    INSERT INTO raw_events (batch_id, adapter, raw_event, ingested_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        adapter_name,
                        json.dumps(event, sort_keys=True),
                        now,
                    ),
                )

    def add_silver_events(
        self,
        batch_id: int,
        valid_events: list[dict[str, Any]],
        quarantined_events: list[dict[str, Any]],
    ) -> None:
        with self._connect() as conn:
            for event in valid_events:
                conn.execute(
                    """
                    INSERT INTO silver_events (
                        batch_id, event_id, timestamp, source, user_id, status,
                        is_valid, validation_errors, raw_event
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 1, NULL, ?)
                    """,
                    (
                        batch_id,
                        event.get("event_id"),
                        event.get("timestamp"),
                        event.get("source"),
                        event.get("user_id"),
                        event.get("status"),
                        json.dumps(event, sort_keys=True),
                    ),
                )
            for row in quarantined_events:
                event = row.get("event", {})
                errors = row.get("errors", [])
                conn.execute(
                    """
                    INSERT INTO silver_events (
                        batch_id, event_id, timestamp, source, user_id, status,
                        is_valid, validation_errors, raw_event
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        batch_id,
                        event.get("event_id"),
                        event.get("timestamp"),
                        event.get("source"),
                        event.get("user_id"),
                        event.get("status"),
                        json.dumps(errors),
                        json.dumps(event, sort_keys=True),
                    ),
                )

    def add_rolling_metrics(self, batch_id: int, metrics: dict[str, float], source: str = "all") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO rolling_metrics (
                    computed_at, batch_id, source, volume, unique_users,
                    error_rate, duplicate_ratio, null_ratio
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    batch_id,
                    source,
                    metrics["volume"],
                    metrics["unique_users"],
                    metrics["error_rate"],
                    metrics["duplicate_ratio"],
                    metrics["null_ratio"],
                ),
            )

    def add_incident(self, incident: dict[str, Any]) -> int:
        recommendation = self.recommendation_for_rule(str(incident["rule"]))
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO incidents (
                    detected_at, resolved_at, rule, severity, source,
                    evidence, recommendation, status
                )
                VALUES (?, NULL, ?, ?, ?, ?, ?, 'open')
                """,
                (
                    incident["detected_at"],
                    incident["rule"],
                    incident["severity"],
                    incident["source"],
                    incident["evidence"],
                    recommendation,
                ),
            )
            incident_id = int(cur.lastrowid)

        for key, value in incident.get("evidence_map", {}).items():
            self.add_incident_evidence(incident_id, str(key), str(value))
        return incident_id

    def add_incident_evidence(self, incident_id: int, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO incident_evidence (incident_id, evidence_key, evidence_value)
                VALUES (?, ?, ?)
                """,
                (incident_id, key, value),
            )

    def resolve_incident(self, incident_id: int, resolved_at: str | None = None) -> None:
        if resolved_at is None:
            resolved_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE incidents
                SET status = 'resolved', resolved_at = ?
                WHERE incident_id = ?
                """,
                (resolved_at, incident_id),
            )

    def simulate_resolves(self, max_resolves: int = 1) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT incident_id, detected_at
                FROM incidents
                WHERE status = 'open'
                ORDER BY incident_id ASC
                LIMIT ?
                """,
                (max_resolves,),
            ).fetchall()

        for row in rows:
            detected_at = datetime.fromisoformat(str(row["detected_at"]))
            resolved_at = (detected_at + timedelta(minutes=5)).isoformat()
            self.resolve_incident(int(row["incident_id"]), resolved_at=resolved_at)
        return len(rows)

    def get_recent_incidents(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT incident_id, detected_at, resolved_at, rule, severity, source,
                       evidence, recommendation, status
                FROM incidents
                ORDER BY incident_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def recommendation_for_rule(rule: str) -> str:
        mapping = {
            "volume_drop": "Verify upstream ingest jobs and source connector health.",
            "duplicate_surge": "Check producer retry behavior and idempotency keys.",
            "null_spike": "Inspect schema changes and nullable source fields.",
            "outlier_volume": "Validate traffic shift or bot activity anomalies.",
            "schema_drift": "Review producer schema contract and downstream mappings.",
        }
        return mapping.get(rule, "Inspect incident evidence and runbook.")
