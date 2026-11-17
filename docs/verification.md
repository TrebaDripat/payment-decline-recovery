# Verification

Local run on 2026-11-17, Linux amd64, Go 1.27.4. The implementation is a new synthetic portfolio example.

| Check | Observed result |
| --- | --- |
| `gofmt` | Applied; formatting check clean |
| `go vet ./...` | Passed |
| `go test -race -count=1 -cover ./...` | Passed; recovery package statement coverage 78.7%; no race report |
| Four independent worker processes | 24 synthetic invoices, 24 provider effects, 48 total attempts including original failures, no pending jobs |
| Concurrent webhook writers | 40 distinct event IDs for one failed attempt, one retry job |
| `go build -o bin/recovery ./cmd/recovery` | Passed |
| `./bin/recovery demo` | Passed; 11 webhook deliveries, 2 events, 1 retry/provider effect, final invoice paid |
| Real loopback HTTP smoke | Started built server; health 200; signed event accepted; repeat marked duplicate; snapshot stayed at 1 event / 1 attempt / 1 job |

The CLI package has no unit-test coverage (reported 0.0%); it was exercised by the compiled demo and HTTP smoke. The normal test run skips the worker-helper test, which is explicitly executed in four subprocesses by the parent integration test.

The crash scenario deliberately omits the engine's final commit after the simulated provider has committed; a fresh database connection reclaims the lease. It does not claim to test a host power failure or SQLite storage corruption.

No real payment provider, card network, production load, multi-host deployment, Windows/macOS runtime or externally hosted service was tested. Provider request deduplication is tested against the durable simulated gateway only.

GitHub Actions results are available from the repository's Actions tab. Local results above do not imply that any later commit passed CI.
