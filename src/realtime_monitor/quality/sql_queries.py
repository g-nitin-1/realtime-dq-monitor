from __future__ import annotations

import sqlite3
from typing import Any


def rolling_window_metrics(conn: sqlite3.Connection, window_size: int = 3) -> list[dict[str, Any]]:
    rows_back = max(window_size - 1, 0)
    sql = f"""
    SELECT
        batch_id,
        computed_at,
        source,
        volume,
        unique_users,
        error_rate,
        AVG(volume) OVER (
            PARTITION BY source
            ORDER BY batch_id
            ROWS BETWEEN {rows_back} PRECEDING AND CURRENT ROW
        ) AS volume_mavg,
        AVG(error_rate) OVER (
            PARTITION BY source
            ORDER BY batch_id
            ROWS BETWEEN {rows_back} PRECEDING AND CURRENT ROW
        ) AS error_rate_mavg
    FROM rolling_metrics
    ORDER BY batch_id ASC
    """
    out = conn.execute(sql).fetchall()
    return [dict(row) for row in out]


def anomaly_summary(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    sql = """
    SELECT
        rule,
        severity,
        strftime('%Y-%m-%dT%H:00:00Z', detected_at) AS time_bucket,
        COUNT(*) AS incident_count
    FROM incidents
    GROUP BY rule, severity, time_bucket
    ORDER BY time_bucket DESC, incident_count DESC
    """
    out = conn.execute(sql).fetchall()
    return [dict(row) for row in out]


def rolling_window_metrics_5m_1h_1d(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    sql = """
    WITH normalized AS (
        SELECT
            source,
            datetime(replace(replace(timestamp, 'T', ' '), 'Z', '')) AS event_ts
        FROM silver_events
        WHERE is_valid = 1 AND timestamp IS NOT NULL
    ),
    latest AS (
        SELECT MAX(event_ts) AS max_ts FROM normalized
    )
    SELECT
        n.source,
        SUM(CASE WHEN n.event_ts >= datetime(l.max_ts, '-5 minutes') THEN 1 ELSE 0 END) AS volume_5m,
        SUM(CASE WHEN n.event_ts >= datetime(l.max_ts, '-1 hour') THEN 1 ELSE 0 END) AS volume_1h,
        SUM(CASE WHEN n.event_ts >= datetime(l.max_ts, '-1 day') THEN 1 ELSE 0 END) AS volume_1d
    FROM normalized n
    CROSS JOIN latest l
    GROUP BY n.source
    ORDER BY volume_1d DESC
    """
    out = conn.execute(sql).fetchall()
    return [dict(row) for row in out]


def sla_time_to_detect_resolve(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    sql = """
    WITH first_event AS (
        SELECT
            incident_id,
            MAX(CASE WHEN evidence_key = 'first_seen_at' THEN evidence_value END) AS first_seen_at,
            MAX(CASE WHEN evidence_key = 'first_event_timestamp' THEN evidence_value END) AS first_event_ts
        FROM incident_evidence
        GROUP BY incident_id
    )
    SELECT
        i.incident_id,
        i.rule,
        i.severity,
        i.status,
        i.detected_at,
        i.resolved_at,
        CASE
            WHEN f.first_seen_at IS NOT NULL THEN
                CAST((julianday(i.detected_at) - julianday(f.first_seen_at)) * 86400 AS INTEGER)
            WHEN f.first_event_ts IS NOT NULL THEN
                CAST((julianday(i.detected_at) - julianday(f.first_event_ts)) * 86400 AS INTEGER)
            ELSE NULL
        END AS time_to_detect_sec,
        CASE
            WHEN i.resolved_at IS NULL THEN NULL
            ELSE CAST((julianday(i.resolved_at) - julianday(i.detected_at)) * 86400 AS INTEGER)
        END AS time_to_resolve_sec
    FROM incidents i
    LEFT JOIN first_event f ON f.incident_id = i.incident_id
    ORDER BY i.incident_id DESC
    """
    out = conn.execute(sql).fetchall()
    return [dict(row) for row in out]
