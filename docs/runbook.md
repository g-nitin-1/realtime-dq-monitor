# Runbook

## 1) Prepare real data events
Example with NYC taxi parquet:
`python3 scripts/prepare_nyc_taxi_events.py --rows 25000 --output data/events_nyc_taxi.jsonl`

## 2) Run monitor (default thresholds)
`PYTHONPATH=src python3 -m realtime_monitor.cli run --input data/events_nyc_taxi.jsonl --adapter canonical`

## 2b) Run monitor for GitHub event payloads
`PYTHONPATH=src python3 -m realtime_monitor.cli run --input data/github_events.jsonl --adapter github`

## 3) Run monitor (tuned thresholds for incident demo)
`PYTHONPATH=src python3 -m realtime_monitor.cli run --reset-state --input data/events_nyc_taxi.jsonl --min-volume-threshold 55 --duplicate-ratio-threshold 0.005 --schema-drift-threshold 0.01 --batch-size 50`

## 4) Micro-batch simulation controls
- `--max-batches N`: process only N batches in this run.
- `--sleep-seconds S`: wait S seconds between batches.
- `--resolve-limit N`: simulate resolution of N open incidents.

## 5) Reports
- Rolling averages: `PYTHONPATH=src python3 -m realtime_monitor.cli report --type rolling`
- Explicit windows (5m/1h/1d): `PYTHONPATH=src python3 -m realtime_monitor.cli report --type windows`
- Anomaly summary: `PYTHONPATH=src python3 -m realtime_monitor.cli report --type anomaly`
- SLA report: `PYTHONPATH=src python3 -m realtime_monitor.cli report --type sla`

## 6) Optional dashboard
1. `python3 -m pip install -e .[ui]`
2. `streamlit run src/realtime_monitor/dashboard.py`

## Outputs
- Database: `data/incidents.db`
- Digest: `data/daily_digest.md`
