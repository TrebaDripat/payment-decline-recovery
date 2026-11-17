# Verification

Reconciliation update verified locally on 2026-11-17, Linux amd64, Go 1.27.4. The implementation is a synthetic portfolio example; no real payment provider or card data was used.

| Check | Observed result |
| --- | --- |
| `gofmt` | Applied; formatting check clean |
| `go vet ./...` | Passed |
| `go test -race -count=1 -cover ./...` | Passed; no race report. Package coverage: recovery 69.5%, HTTP server 85.7%, simulator 72.1% |
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

The CLI package has no unit-test coverage (reported 0.0%); the compiled demo and three-process smoke exercise it. Coverage numbers above are Go's per-package test measurements; they do not merge cross-package HTTP tests or helper-subprocess execution into every package's percentage.

Two helper tests are skipped in their ordinary parent run and explicitly executed as subprocesses: the existing four-worker test and the new kill/restart HTTP test. The latter really terminates the worker process, while advancing the engine's logical clock to reclaim its lease without a 30-second sleep. The provider runs independently from the killed worker and stores its receipt in a separate SQLite file. The smoke script advances the due time of its disposable fixture before starting the worker to avoid a one-minute scheduling wait.

No host power failure, storage corruption, real payment provider, card network, production load, multi-host deployment, Windows/macOS runtime or externally hosted service was tested. Provider deduplication and authoritative lookup are verified against the simulator's documented contract.

GitHub Actions results are available from the repository's Actions tab. Local results above do not imply that any later commit passed CI.
