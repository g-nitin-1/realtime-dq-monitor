# Feature Spec

## Reliability impact
The monitor reduces mean time to detect (MTTD) by scanning every micro-batch for both hard rule violations and baseline deviations.
It reduces mean time to resolve (MTTR) by storing machine-readable evidence and recommended actions with every incident.

## Severity definitions
- P0: critical data contract or availability failure.
  - Triggers: `schema_drift`, severe `volume_drop`.
  - SLA target: immediate triage, acknowledged within 15 minutes.
- P1: high-priority quality degradation.
  - Triggers: `duplicate_surge`, `null_spike`.
  - SLA target: triage within 60 minutes.
- P2: informative anomaly.
  - Trigger: `outlier_volume`.
  - SLA target: review within 1 business day.

## Success criteria (must pass)
1. Alerts:
- Console alerts emit severity, rule, source, incident_id.
- Rate limiter suppresses alert spam during burst windows.

2. Storage:
- Valid and quarantined records are persisted to `silver_events`.
- Batch metrics are persisted to `rolling_metrics`.
- Incidents are persisted to `incidents` and key-value evidence to `incident_evidence`.

3. Analytics:
- Rolling window query works for moving averages.
- 5min/1hr/1day query works for source-level volumes.
- Anomaly summary groups by rule/severity/time bucket.
- SLA query returns time-to-detect and time-to-resolve.

4. QA:
- Unit + integration tests pass.
- Linting and tests run in CI as separate workflows.
