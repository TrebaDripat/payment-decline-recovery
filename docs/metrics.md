# Metrics

`GET /metrics` exposes Prometheus text format 0.0.4 on the engine API. All samples
come from one SQLite read transaction, including changes by independent workers.
Counters survive process restarts. Database replacement, restore or future record
retention can reset them; Prometheus `rate()` handles counter resets. Migrated
legacy inbox records start at one observed delivery per accepted event; earlier
redelivery counts cannot be reconstructed.

| Metric | Meaning |
| --- | --- |
| `recovery_jobs{state,mode}` | Current queued, leased, done and paused jobs; mode is recover or reconcile |
| `recovery_job_claims_total` | Persisted claims, including reclaimed leases; not HTTP calls or payments |
| `recovery_ready_jobs` | Queued jobs due now plus expired leases |
| `recovery_oldest_ready_job_age_seconds` | Age of the oldest currently claimable job; excludes scheduled future work and active leases |
| `recovery_webhook_deliveries_total{decision}` | Valid journaled deliveries, including repeats, grouped by the original decision for that payload |
| `recovery_webhook_redeliveries_total` | Repeated event ID plus identical canonical payload; aliases for the same attempt are counted separately under decision `duplicate` |
| `recovery_payment_attempts_total{status}` | Recorded outcomes, including initial failures delivered by webhook |
| `recovery_invoices{state}` | Current invoice states |

Labels use fixed enums; no invoice, event, job or attempt IDs are exposed. Invalid
or unauthenticated requests are rejected before journaling and excluded. Provider
effects are counted by the independent provider's `/snapshot`, not inferred from
worker claims or payment attempts. HTTP latency is measured by the benchmark;
these metrics do not invent a historical provider error or latency series.

Example scrape configuration for Prometheus **on the same host** as the API:

```yaml
scrape_configs:
  - job_name: payment-recovery
    scrape_interval: 15s
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

Scrape one API instance per database. Multiple API replicas expose the same
database totals; summing them double-counts the work. For larger databases,
measure scrape cost before enabling frequent polling: aggregates scan stored
history. A failed read returns HTTP 503, not zero-valued success samples.

Useful starting queries (thresholds need calibration to actual traffic):

```promql
recovery_oldest_ready_job_age_seconds > 60
sum(recovery_jobs{state="needs_review"}) > 0
sum(recovery_jobs{state=~"queued|leased",mode="reconcile"}) > 0
rate(recovery_webhook_redeliveries_total[5m])
```

The endpoint shares the local demo's unauthenticated diagnostic boundary. Keep it
on loopback or a trusted private network.
