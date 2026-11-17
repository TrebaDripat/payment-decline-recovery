#!/usr/bin/env python3
"""Run the built API, HTTP provider and worker against disposable local databases."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def wait_for(read, predicate, processes):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if any(process.poll() is not None for process, _ in processes):
            raise RuntimeError("a service exited before the scenario completed")
        try:
            value = read()
            if predicate(value):
                return value
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError("scenario did not finish within 15 seconds")


def main():
    executable = Path(__file__).resolve().parents[1] / "bin" / "recovery"
    api_port, provider_port = free_port(), free_port()
    while provider_port == api_port:
        provider_port = free_port()
    api = f"http://127.0.0.1:{api_port}"
    provider = f"http://127.0.0.1:{provider_port}"
    environment = os.environ.copy()
    environment["RECOVERY_WEBHOOK_SECRET"] = "disposable-smoke-test-secret"
    with tempfile.TemporaryDirectory(prefix="recovery-http-smoke-") as directory:
        root = Path(directory)
        engine_db = root / "engine.db"
        processes = []

        def spawn(name, *args):
            log = (root / f"{name}.log").open("w+")
            process = subprocess.Popen([str(executable), *args], env=environment,
                                       stdout=log, stderr=log)
            processes.append((process, log))

        def send(event):
            body = json.dumps(event).encode()
            timestamp = str(int(time.time()))
            signature = hmac.new(environment["RECOVERY_WEBHOOK_SECRET"].encode(),
                                 timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
            request = urllib.request.Request(api + "/webhooks", body, headers={
                "Content-Type": "application/json", "X-Webhook-Timestamp": timestamp,
                "X-Webhook-Signature": signature,
            })
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.load(response)

        succeeded = False
        try:
            spawn("provider", "provider", "-db", str(root / "provider.db"),
                  "-listen", f"127.0.0.1:{provider_port}", "-fault", "drop_after_commit")
            spawn("api", "serve", "-db", str(engine_db), "-listen", f"127.0.0.1:{api_port}")
            wait_for(lambda: get(api + "/health"), lambda v: v["status"] == "ok", processes)
            wait_for(lambda: get(provider + "/snapshot"), lambda v: v["provider_effects"] == 0, processes)
            event = dict(event_id="delivery-a", invoice_id="synthetic-invoice",
                         attempt_id="initial-attempt", status="failed", code="insufficient_funds")
            assert send(event)[1]["status"] == "processed"
            assert send(event)[1]["duplicate_delivery"] is True
            alias = dict(event, event_id="delivery-b")
            assert send(alias)[1]["status"] == "duplicate"
            # Advance only this disposable fixture's due time, before starting
            # the worker; avoid a one-minute wait without changing runtime policy.
            with sqlite3.connect(engine_db) as connection:
                connection.execute("UPDATE jobs SET due_at=0 WHERE state='queued'")
            spawn("worker", "worker", "-db", str(engine_db), "-gateway", provider)
            final = wait_for(lambda: get(api + "/snapshot"),
                             lambda v: v["invoices"][0]["state"] == "paid", processes)
            assert final["payment_attempts"] == 2 and final["pending_jobs"] == 0
            assert get(provider + "/snapshot")["provider_effects"] == 1
            conflict = dict(event, status="succeeded", code="")
            status, receipt = send(conflict)
            assert status == 202 and receipt["status"] == "needs_review"
            inbox = get(api + "/inbox")
            assert len(inbox) == 3 and sum(e["deliveries"] for e in inbox) == 4
            assert len([e for e in inbox if e["status"] == "needs_review"]) == 1
            assert get(api + "/snapshot")["invoices"][0]["state"] == "paid"
            succeeded = True
        finally:
            cleanup_errors = []
            for process, log in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                    cleanup_errors.append("shutdown exceeded deadline")
                if process.returncode != 0:
                    cleanup_errors.append(f"service exit status {process.returncode}")
                log.close()
            if succeeded and cleanup_errors:
                raise RuntimeError(", ".join(cleanup_errors))
    print(json.dumps({"http_process_smoke": "passed", "processes": 3,
                      "provider_effects": 1, "payment_attempts": 2,
                      "conflict_http_status": 202, "graceful_shutdown": "passed"}, indent=2))


if __name__ == "__main__":
    main()
