# Sample Postmortem - Duplicate Surge

- Incident ID: 742
- Severity: P1
- Detection timestamp: 2026-03-02T09:12:53+00:00
- Start timestamp: 2026-03-02T09:12:50+00:00
- Resolve timestamp: 2026-03-02T09:17:53+00:00
- Time to detect (seconds): 3
- Time to resolve (seconds): 300
- Customer/business impact: delayed trust in dashboard totals for one source window.
- Root cause: producer retry path emitted duplicate payloads without idempotency key enforcement.
- Detection gaps: no producer-side duplicate budget alert.
- Corrective actions: add producer de-duplication guard and retry jitter.
- Preventive actions: enforce event_id uniqueness contract and monitor duplicate ratio weekly.
- Owner and due dates: Data Platform Team, 2026-03-09.
