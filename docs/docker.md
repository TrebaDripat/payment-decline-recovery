# Run with Docker Compose

Requires Docker with Compose v2 and Python 3 only for sending the demo webhook.

```sh
cp .env.example .env
docker compose up --build --detach --wait
RECOVERY_WEBHOOK_SECRET=disposable-compose-demo-secret python3 scripts/send_webhook.py
curl http://127.0.0.1:8080/snapshot
curl http://127.0.0.1:8080/metrics
curl http://127.0.0.1:8090/snapshot
```

Use the secret from your `.env` if you changed it. The first job becomes due after
one minute. By default, the simulator commits its receipt and drops the first
response; the worker then looks up the result and marks the invoice paid without
another provider effect. Watch progress with `docker compose logs -f worker`.
Set `RECOVERY_PROVIDER_FAULT=none` in `.env` for the normal response path.

The API and provider publish **loopback-only** host ports (8080 and 8090). Change
`RECOVERY_API_PORT` and `RECOVERY_PROVIDER_PORT` in `.env` if those ports are busy.
The worker uses the provider's internal service name. All services run as UID
10001, with read-only root filesystems and separate writable named volumes:
the API/workers share `engine-data`; only the simulator uses `provider-data`.
Named volumes receive the image's `/data` ownership on first use. Bind mounts
require that directory to be writable by UID 10001. Build context excludes `.env`,
database files, Git history and unrelated local files.

```sh
# Competing workers share one local SQLite database.
docker compose up --detach --scale worker=4 --wait
docker compose exec api recovery inbox -db /data/engine.db
docker compose exec api recovery reviews -db /data/engine.db
# Stop services while preserving both databases.
docker compose down
```

`docker compose down --volumes` **deletes this Compose project's demo data**.
Do not reset only the provider's volume while retaining pending engine work: its
idempotency history is part of the recovery contract. SQLite is a single-host
demonstration; do not share these files across hosts or network filesystems.

## Automated check

```sh
python3 scripts/compose_smoke.py
```

The check builds the image and starts an isolated project on temporary loopback
ports with two competing workers. It waits through the real one-minute retry
delay, verifies lost-response reconciliation and one provider effect, checks
metrics and graceful stop, restarts all containers, and verifies persisted
deduplication. Its own project volumes are removed even on failure. The image
remains cached for later builds. GitHub Actions runs this check separately from
the Go tests. Expect a few minutes for the first build and about a minute for the
scenario itself.

On a network with a custom TLS proxy, supply a trusted PEM CA bundle as an
optional BuildKit secret. It is used for dependency downloads, never copied into
the image. TLS verification stays enabled:

```sh
docker build --secret id=build_ca,src=/path/to/trusted-ca-bundle.pem -t payment-decline-recovery:local .
python3 scripts/compose_smoke.py --skip-build
# Or run the prebuilt image interactively:
docker compose up --detach --no-build --wait
```
