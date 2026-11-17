# Reproducible synthetic benchmark

```sh
go build -o bin/recovery ./cmd/recovery
python3 scripts/benchmark.py --output benchmark-local.json
```

The default run uses 40 distinct invoices, three signed webhook deliveries per
invoice, eight concurrent HTTP clients and three repetitions with one and four
**compiled CLI worker processes**. Every repetition starts fresh engine and
provider databases. It verifies exactly one provider effect and two recorded
attempts (initial failure plus success) per invoice, all invoices paid, no pending
or review jobs, and the expected duplicate-delivery count. Process failures,
timeouts and failed invariants fail the command; temporary files are removed and
all child processes are stopped, including on failure.

The report includes per-run webhook p50/p95/max client latency (nearest rank),
ingestion throughput, recovery wall time and throughput, environment metadata,
binary hash, source revision and whether the checkout had uncommitted changes.
The provider runs on loopback with SQLite persistence and no injected fault.
Webhook latency includes HTTP connection setup and response decoding; clients do
not reuse connections. Ingestion and recovery run in separate phases.

Only initial due times in disposable fixtures are advanced before workers start,
excluding the intentional one-minute business delay. Recovery timing includes
worker startup and completion polling at 50 ms. Workers retain the actual 250 ms
poll interval and execute one job per tick, so the immediate-response ceiling is
approximately **four jobs per second per worker**. This benchmark makes that
limitation visible. It is not a database capacity test or evidence of production
payment throughput, real network latency or a load-related business improvement.

## Observed sample: 2026-11-17

[Raw results](benchmark-2026-11-17.json) were collected with Go 1.27.4 on Linux
amd64, AMD EPYC 9V74, four CPU equivalents of cgroup quota and 16 GiB memory limit.
The recorded revision identifies the runtime; the checkout was dirty because
this benchmark script and its documentation were being added. The binary's
SHA-256 is retained in the report.

| Workers | Median recovered invoices/s, three runs | Per-run webhook p95 range |
| --- | --- | --- |
| 1 | 3.989 | 12.047–22.987 ms |
| 4 | 15.753 | 13.378–14.962 ms |

All six runs recovered 40 invoices with exactly 40 provider effects and 80
recorded attempts, with no pending jobs. Every run accepted 120 signed deliveries
and identified 80 redeliveries. The short batches primarily expose the worker
polling limit; these are observed local samples, not an SLA or production claim.

For a quick correctness run, as used in CI:

```sh
python3 scripts/benchmark.py --invoices 8 --workers 1,2 --runs 1
```

CI asserts invariants, not timing thresholds. Shared runners vary; compare raw
samples on the same machine before drawing performance conclusions. For faults,
use the connection-loss, deadline and process-kill tests described in
[verification](verification.md); the throughput run covers the success path.
