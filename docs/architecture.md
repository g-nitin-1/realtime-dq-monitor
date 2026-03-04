# Architecture

## Flow
source -> adapter -> validation -> raw/silver/quarantine storage -> metrics -> detection -> incidents/evidence -> alerting -> analytics

```mermaid
flowchart LR
    A["JSONL Source"] --> B["Adapter"]
    B --> C["Validation"]
    B --> D["raw_events"]
    C -->|valid| E["silver_events"]
    C -->|invalid or quarantine| E
    E --> F["Metrics"]
    F --> G["Detection"]
    G --> H["incidents"]
    G --> I["incident_evidence"]
    H --> J["Alerting"]
    E --> K["Analytics"]
    F --> K
    H --> K
    I --> K
    K --> L["Daily Digest"]
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
