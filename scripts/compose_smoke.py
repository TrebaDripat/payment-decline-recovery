#!/usr/bin/env python3
"""Build and exercise a disposable Compose project, then remove only its volumes."""
import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request
import uuid


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true", help="use an explicitly prebuilt local image")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    project = "recovery-smoke-" + uuid.uuid4().hex[:10]
    api_port, provider_port = port(), port()
    while provider_port == api_port:
        provider_port = port()
    secret = "disposable-compose-smoke-secret"
    environment = dict(os.environ, RECOVERY_WEBHOOK_SECRET=secret,
                       RECOVERY_API_PORT=str(api_port), RECOVERY_PROVIDER_PORT=str(provider_port),
                       RECOVERY_PROVIDER_FAULT="drop_after_commit")
    command = ["docker", "compose", "--project-name", project, "--file", str(root / "compose.yaml")]

    def compose(*args, timeout=120, capture=False):
        return subprocess.run(command + list(args), cwd=root, env=environment,
                              check=True, timeout=timeout, text=True, capture_output=capture)

    def get(base, path):
        with urllib.request.urlopen(base + path, timeout=5) as response:
            return json.load(response)

    api, provider = f"http://127.0.0.1:{api_port}", f"http://127.0.0.1:{provider_port}"

    def send():
        body = json.dumps(dict(event_id="compose-delivery", invoice_id="compose-invoice",
                               attempt_id="compose-attempt", status="failed", code="insufficient_funds")).encode()
        timestamp = str(int(time.time()))
        signature = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
        request = urllib.request.Request(api + "/webhooks", body, headers={
            "Content-Type": "application/json", "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": signature,
        })
        with urllib.request.urlopen(request, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("webhook was not accepted")
            return json.load(response)

    try:
        if not args.skip_build:
            compose("build", "api", timeout=600)
        compose("up", "--detach", "--no-build", "--wait", "--wait-timeout", "60", "--scale", "worker=2")
        assert get(api, "/health")["status"] == "ok"
        assert get(api, "/openapi.json")["openapi"] == "3.1.0"
        assert get(provider, "/openapi.json")["openapi"] == "3.1.0"
        assert send()["status"] == "processed"
        assert send()["duplicate_delivery"] is True
        # Exercise the real one-minute retry delay; no fixture DB edits here.
        deadline = time.monotonic() + 100
        while time.monotonic() < deadline:
            final = get(api, "/snapshot")
            if final["invoices"][0]["state"] == "paid":
                break
            time.sleep(0.5)
        else:
            raise TimeoutError("Compose workers did not reconcile within 100 seconds")
        assert final["payment_attempts"] == 2 and final["pending_jobs"] == 0
        assert get(provider, "/snapshot")["provider_effects"] == 1
        with urllib.request.urlopen(api + "/metrics", timeout=5) as response:
            metrics = response.read().decode()
        assert 'recovery_invoices{state="paid"} 1\n' in metrics
        # Restart every service with the same volumes; a redelivery must retain
        # both engine history and the independent provider's idempotent receipt.
        compose("stop")
        records = compose("ps", "--all", "--format", "json", capture=True).stdout.strip()
        containers = json.loads(records) if records.startswith("[") else [json.loads(line) for line in records.splitlines()]
        assert len(containers) == 4 and all(c["ExitCode"] == 0 for c in containers), containers
        compose("up", "--detach", "--no-build", "--wait", "--wait-timeout", "60", "--scale", "worker=2")
        assert send()["duplicate_delivery"] is True
        assert get(api, "/snapshot")["payment_attempts"] == 2
        assert get(provider, "/snapshot")["provider_effects"] == 1
        print(json.dumps(dict(compose_smoke="passed", worker_containers=2,
                              provider_effects=1, payment_attempts=2,
                              persisted_after_restart=True, graceful_shutdown="passed"), indent=2))
    except BaseException:
        subprocess.run(command + ["logs", "--no-color", "--tail", "80"], cwd=root, env=environment, timeout=30)
        raise
    finally:
        # Unique project name: never remove a developer's default demo volumes.
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
