# Verification

Reconciliation, observability and API-contract updates verified locally on 2026-11-17, Linux amd64, Go 1.27.4. The implementation is a synthetic portfolio example; no real payment provider or card data was used.

| Check | Observed result |
| --- | --- |
| `gofmt` | Applied; formatting check clean |
| `go vet ./...` | Passed |
| `go test -race -count=1 -cover ./...` | Passed; no race report. Package coverage: recovery 72.0%, HTTP server 85.7%, simulator 72.4% |
| Four independent worker processes | 24 synthetic invoices, 24 provider effects, 48 total attempts including original failures, no pending jobs |
| Concurrent webhook writers | 40 distinct event IDs for one failed attempt, one retry job |
| Durable conflict inbox | Conflicting payload retained with reason and occurrence count; active job paused; records survive reopening |
| Unknown-result policy | Receipt lookup avoids another POST; authoritative not-found reuses the original key; unresolved work pauses at the persisted deadline |
| Real process kill over HTTP | Worker killed after the independent provider commits; restarted worker makes one lookup; total one POST, one provider effect, final invoice paid |
| HTTP faults | Real dropped connection and response deadline; rate-limit and unavailable/rejected responses; malformed responses and redirects rejected |
| Migration | Legacy events/receipts preserved, uncertain legacy jobs paused, migration repeat-safe; invalid legacy data rolls back schema changes |
| Graceful shutdown | In-flight HTTP request finishes before server returns; deadline closes the connection and cancels the request context |
| `go build -o bin/recovery ./cmd/recovery` | Passed |
| `./bin/recovery demo` | Passed over loopback HTTP with separate database files: 11 deliveries, 2 events, 1 provider effect after lost response and after reconciliation, invoice paid |
| `python3 scripts/smoke.py` | Passed with three compiled processes (API/provider/worker); signed delivery and duplicate; lost-response recovery; conflict acknowledged with 202 and journaled; graceful process termination |
| Prometheus metrics | Cross-connection writes visible in one snapshot, counters retained after reopen, future retries excluded from queue lag, expired leases included, failed scrape returns 503 |
| Synthetic benchmark | Six default runs passed all payment/delivery invariants. Median recovery: 3.989 invoices/s with one worker, 15.753 with four. [Raw samples and methodology](benchmark.md) |
| Docker Compose | Image built with Go 1.26.10; two worker containers, real one-minute scheduling delay, lost-response reconciliation, one provider effect, clean stop and persisted deduplication after restart |
| OpenAPI 3.1 | Two documents validated; 43 real compiled-service responses checked against schemas, including webhook conflicts and provider errors |

The CLI and embedded-document packages have no direct Go unit-test coverage (reported 0.0%); the compiled demo, smoke and live contract check exercise them. Coverage numbers above are Go's per-package test measurements; they do not merge cross-package HTTP tests, Python-driven executable checks or helper-subprocess execution into every package's percentage.

Two helper tests are skipped in their ordinary parent run and explicitly executed as subprocesses: the existing four-worker test and the new kill/restart HTTP test. The latter really terminates the worker process, while advancing the engine's logical clock to reclaim its lease without a 30-second sleep. The provider runs independently from the killed worker and stores its receipt in a separate SQLite file. The smoke script advances the due time of its disposable fixture before starting the worker to avoid a one-minute scheduling wait.

No host power failure, storage corruption, real payment provider, card network, production load, multi-host deployment, Windows/macOS runtime or externally hosted service was tested. Provider deduplication and authoritative lookup are verified against the simulator's documented contract.

The local Docker build needed the environment's trusted CA bundle as a temporary
BuildKit secret for dependency downloads. TLS verification remained enabled; the
bundle is not included in the runtime image. Docker 29.9.2 and Compose v2.40.3
were used. The Compose smoke removes only its uniquely named test project and
volumes. [Reproduction commands](docker.md).

GitHub Actions results are available from the repository's Actions tab. Local results above do not imply that any later commit passed CI.
