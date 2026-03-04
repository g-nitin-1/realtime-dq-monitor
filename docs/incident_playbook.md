# Incident Playbook

## Triage steps
1. Confirm alert details: severity, rule, source, incident_id, detection timestamp.
2. Pull evidence from `incident_evidence` for batch id and ratios.
3. Validate upstream producer and schema contract status.
4. Determine blast radius: affected sources, time buckets, and downstream datasets.
5. Mitigate, verify recovery, and document root cause.

## Severity response
- P0:
  - Immediate owner paging.
  - Freeze downstream publish if required.
  - Open incident bridge and status updates.
- P1:
  - Assign within on-call hour.
  - Validate data repair/replay options.
- P2:
  - Track in backlog with trend monitoring.

## Resolution checklist
- Incident status set to `resolved`.
- Resolution timestamp recorded.
- Corrective and preventive actions documented.
- Postmortem created for P0/P1.
