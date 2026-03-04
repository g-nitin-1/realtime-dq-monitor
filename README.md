---
title: Realtime DQ Monitor
sdk: streamlit
app_file: src/realtime_monitor/dashboard.py
---

# realtime-dq-monitor

Realtime (micro-batch) data-quality monitor with incident detection, alerting, SQL analytics, and dashboard analytics.

## Quickstart
1. `python3 -m venv .venv && source .venv/bin/activate`
2. `pip install -e .[dev]`
3. `pytest -q`
4. Run monitor:
   `PYTHONPATH=src python3 -m realtime_monitor.cli run --input data/events.jsonl --adapter canonical`
   or for GitHub event payloads:
   `PYTHONPATH=src python3 -m realtime_monitor.cli run --input data/github_events.jsonl --adapter github`
5. Print reports:
  - `PYTHONPATH=src python3 -m realtime_monitor.cli report --type rolling`
  - `PYTHONPATH=src python3 -m realtime_monitor.cli report --type windows`
  - `PYTHONPATH=src python3 -m realtime_monitor.cli report --type anomaly`
  - `PYTHONPATH=src python3 -m realtime_monitor.cli report --type sla`
6. Optional dashboard:
   - `python3 -m pip install -e .[ui]`
   - `streamlit run src/realtime_monitor/dashboard.py`

## Implemented
- Pipeline: ingest, schema validation, quarantine handling, metrics, rules/detectors, alerting.
- Adapter layer: `canonical` and `github` adapters map raw payloads to canonical schema.
- Storage tables: `raw_events`, `silver_events`, `rolling_metrics`, `incidents`, `incident_evidence` in SQLite.
- SQL analytics: rolling-window metrics, explicit 5m/1h/1d windows, anomaly summary, SLA-style time-to-detect/time-to-resolve.
- Daily digest: Polars-backed markdown digest (`data/daily_digest.md`).
- Configurable micro-batch simulation: batch size, max batches, sleep interval, threshold flags.
- Optional Streamlit dashboard for incidents/metrics browsing.
- CI split into three workflows:
  - `.github/workflows/lint.yml`
  - `.github/workflows/test.yml`
  - `.github/workflows/deploy.yml`
  - deploy creates a GitHub Release on `v*` tags and uploads built artifacts

## Assignment status
- Completion matrix: `docs/assignment_completion_matrix.md`
- Fully automated deliverables are implemented in code/docs.
- Manual artifact still pending: demo video recording.

## Data Flow
- `any dataset payload -> adapter -> canonical event schema -> validation/quarantine -> metrics/incidents`
- Canonical schema: `event_id`, `timestamp`, `source`, `user_id`, `status`
