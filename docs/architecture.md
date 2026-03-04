# Architecture

## Flow
source -> adapter -> validation -> raw/silver/quarantine storage -> metrics -> detection -> incidents/evidence -> alerting -> analytics

```mermaid
flowchart LR
    A[JSONL Micro-Batch Source] --> B[Adapter Layer]
    B --> C[Schema Validation]
    B --> D[raw_events]
    C -->|valid| E[silver_events]
    C -->|invalid/quarantine| E
    E --> F[Batch Metrics]
    F --> G[Detectors]
    G --> H[incidents]
    G --> I[incident_evidence]
    H --> J[Console Alert + Adapter]
    E --> K[SQL Analytics]
    F --> K
    H --> K
    I --> K
    K --> L[Daily Digest (Polars)]
```

## Storage tables
- `raw_events`: raw event payload captured before canonical mapping/validation.
- `silver_events`: normalized event store with validity/quarantine metadata.
- `rolling_metrics`: per-batch metrics snapshots.
- `incidents`: incident header with severity/status/recommendation.
- `incident_evidence`: key-value evidence attached to incidents.

## Idempotency strategy
- Checkpoint offset (`data/checkpoint.json`) records last processed record.
- Re-runs resume from offset and avoid re-processing prior micro-batches.
- Duplicate trends are still detectable via `event_id` collision rate.

## Analytics support
- Rolling moving-average window query on `rolling_metrics`.
- Explicit 5min/1hr/1day volume windows from event timestamps.
- Anomaly summary grouped by rule, severity, and hour bucket.
- SLA query with time-to-detect/time-to-resolve from incident evidence and status.
