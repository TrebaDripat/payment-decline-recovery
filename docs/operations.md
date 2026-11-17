# Review and migration runbook

All commands in this repository operate on synthetic local data. The simulator is not a production payment adapter.

## Inspect unresolved work

```sh
./bin/recovery reviews -db recovery.db
./bin/recovery inbox -db recovery.db
./bin/recovery snapshot -db recovery.db
```

The diagnostic lists are bounded to 100 records. The inbox keeps canonical payload variants and occurrence counts. It is not a raw webhook archive or an unlimited export API.

| Reason | Interpretation | Investigation |
| --- | --- | --- |
| `resolution_window_elapsed` | The 15-minute per-job resolution deadline expired | Look up the provider receipt using the job ID and invoice ID; preserve both databases |
| `provider_rejected_request` | Provider permanently rejected the request or lookup | Inspect the adapter contract and configuration; do not mint another key |
| `invalid_provider_receipt` | Provider returned an unsupported terminal result | Inspect the provider record and protocol version |
| `webhook_conflict` | An event contradicted the known invoice/attempt history | Compare the inbox payload variants with the provider's authoritative history |
| `legacy_delivery_requires_review` | A v0.1 job may already have reached its embedded provider | Inspect the retained legacy receipt and original engine database before any decision |

For a current HTTP-provider job, a read-only lookup is:

```sh
curl 'http://127.0.0.1:8090/recoveries/JOB_ID?invoice_id=INVOICE_ID'
```

Replace both identifiers with the review record's values. A legacy job's receipt is in the engine's retained `gateway_receipts` table, not automatically transferred to the new HTTP provider.

Manual review is a terminal automation boundary in this demo. There is deliberately no resume or overwrite endpoint. A real operator-resolution feature needs an authenticated actor, an audited decision, provider evidence and a policy for correcting invoice state. Deleting a job, resetting its delivery count or changing its key does not resolve an unknown payment.

Pausing an invoice stops future engine dispatch and fences stale local commits. It cannot recall a request already in flight at the external provider. If that request succeeds after the pause, the receipt remains available for review.

## Upgrade an existing v0.1 database

1. Stop every old API and worker process. Never run old and new binaries against the same database.
2. Back up the existing database using SQLite's backup API. Example:

   ```sh
   python3 - <<'PY'
   import sqlite3
   with sqlite3.connect('file:recovery.db?mode=ro', uri=True) as source:
       with sqlite3.connect('recovery.before-v2.db') as target:
           source.backup(target)
   PY
   ```

3. Build the new binary and run `./bin/recovery snapshot -db recovery.db`. The first open migrates under one `BEGIN IMMEDIATE` transaction.
4. Inspect `reviews` and `inbox`. Previously accepted events are backfilled with delivery count 1; historical redelivery counts cannot be recovered.
5. Start the HTTP provider using its own new file, then start the new API and worker. Do not point both services at the same file.

The migration retains accepted events, invoices, immutable payment attempts, completed jobs and embedded-provider receipts. It pauses every unfinished legacy job with prior deliveries, because a new provider has no authoritative knowledge of those calls. Never-dispatched queued jobs remain queued. No legacy receipt is copied to the HTTP provider, and no uncertain legacy job is automatically charged again.

Migration errors roll back schema and data changes. Unknown future schema versions are refused. To roll back the application version, stop all new processes and restore the pre-upgrade backup; an old binary is not a schema downgrade tool.

## Restart behavior

Keep the provider database as carefully as the engine database: the provider's receipt is the evidence behind idempotency. On an engine-worker crash, an expired lease is reclaimed in reconciliation mode and the worker first looks up the same key. After 15 minutes from first claim, unresolved work is paused for review. Repeated claims never reset this deadline.

The standalone API and provider drain active HTTP requests on SIGTERM for up to five seconds, then close outstanding connections and exit with an error if draining exceeded the deadline. The worker cancels its current operation; any incomplete lease is recoverable after 30 seconds.

The HTTP simulator retains receipts indefinitely and provides authoritative lookup. This contract does not describe every real payment platform. Storage loss, expiring idempotency retention, eventual consistency, changing provider endpoints and external/manual payments require explicit integration policies.
