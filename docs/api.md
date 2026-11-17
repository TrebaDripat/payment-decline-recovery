# OpenAPI contracts

Both services expose their embedded OpenAPI 3.1 contract:

| Service | Live URL (default ports) | Checked-in source |
| --- | --- | --- |
| Recovery engine | `http://127.0.0.1:8080/openapi.json` | [engine.json](../internal/apidocs/engine.json) |
| Synthetic provider | `http://127.0.0.1:8090/openapi.json` | [provider.json](../internal/apidocs/provider.json) |

Import either document into an OpenAPI-compatible client. The default server
URLs assume the local demo; change them if using other ports. No external
documentation host or JavaScript CDN is required. `go:embed` ensures deployed
documentation travels with the binary, including in Docker.

The engine contract covers signed webhooks, durable conflict acknowledgement,
health, snapshots, inbox, paused jobs and Prometheus metrics. The provider contract
covers stable idempotency keys, immutable receipts, authoritative lookup,
identity conflicts, `Retry-After`, and ambiguous transport failures. These are
simulator guarantees, not promises about a real payment provider.

For webhooks, the two required security headers are **computed for each request**:
the signature is hex HMAC-SHA256 over `timestamp + "." + raw_body`. It is not a
fixed API token; a generic client's API-key authorization dialog cannot calculate
it. Use `scripts/send_webhook.py` as the executable signing example, and sign the
exact bytes sent with a current timestamp. All diagnostic endpoints and the
simulator are unauthenticated local demo interfaces.

## Validate documents and implementation together

```sh
python3 -m venv .venv
.venv/bin/pip install -r scripts/requirements-contracts.txt
go build -o bin/recovery ./cmd/recovery
.venv/bin/python scripts/check_api.py
```

The check validates both OpenAPI documents, launches real compiled services with
disposable databases, compares their embedded documents with the checked-in
sources, and validates real response bodies against the declared JSON schemas.
Cases include empty and populated diagnostics, webhook redelivery and conflict,
authentication/validation/body-size failures, successful and declined provider
receipts, conflicting identity, authoritative not-found and injected provider
errors. The live `Retry-After` header is also checked. This runs in CI.

Python dependencies are only validation tools; running the Go service does not
require Python. Go tests separately exercise storage failures, response loss,
deadline ambiguity, process death, fencing and migration. This contract check
does not attempt exhaustive request fuzzing or production-provider validation.
