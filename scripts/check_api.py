#!/usr/bin/env python3
"""Validate OpenAPI documents and real compiled-service responses against them."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from jsonschema import Draft202012Validator
from openapi_spec_validator import validate
from smoke import free_port, get, wait_for

ROOT = Path(__file__).resolve().parents[1]
SECRET = "disposable-contract-check-secret"


def check_value(spec, schema, value):
    # Keep document-local references rooted at the full components dictionary.
    Draft202012Validator(dict(schema, components=spec["components"])).validate(value)


def main():
    specs = {}
    for name in ("engine", "provider"):
        specs[name] = json.loads((ROOT / "internal" / "apidocs" / (name + ".json")).read_text())
        validate(specs[name])
    checked = []
    with tempfile.TemporaryDirectory(prefix="recovery-contracts-") as directory:
        root = Path(directory)
        processes = []
        used_ports = set()

        def spawn(name, command, *args):
            port = free_port()
            while port in used_ports:
                port = free_port()
            used_ports.add(port)
            log = (root / (name + ".log")).open("w+")
            try:
                process = subprocess.Popen([str(ROOT / "bin" / "recovery"), command, "-db",
                                            str(root / (name + ".db")), "-listen", f"127.0.0.1:{port}", *args],
                                           env=dict(os.environ, RECOVERY_WEBHOOK_SECRET=SECRET), stdout=log, stderr=log)
            except BaseException:
                log.close()
                raise
            processes.append((process, log))
            base = f"http://127.0.0.1:{port}"
            wait_for(lambda: get(base + ("/health" if command == "serve" else "/snapshot")),
                     lambda _: True, processes)
            return base

        def request(name, base, path, expected=200, body=None, headers=None, route=None):
            method = "POST" if body is not None else "GET"
            raw = json.dumps(body).encode() if isinstance(body, dict) else body
            headers = dict(headers or {})
            if body is not None:
                headers.setdefault("Content-Type", "application/json")
            req = urllib.request.Request(base + path, raw, headers=headers, method=method)
            try:
                response = urllib.request.urlopen(req, timeout=5)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                data = response.read()
                if response.status != expected:
                    raise AssertionError(f"{method} {path}: expected {expected}, got {response.status}: {data!r}")
                operation = specs[name]["paths"][route or path][method.lower()]
                contract = operation["responses"][str(response.status)]
                media = response.headers.get_content_type()
                schema = contract["content"][media]["schema"]
                value = json.loads(data) if media == "application/json" else data.decode()
                check_value(specs[name], schema, value)
                for header, definition in contract.get("headers", {}).items():
                    if header == "Retry-After":
                        check_value(specs[name], definition["schema"], response.headers[header])
                checked.append(f"{name} {method} {route or path} {response.status}")
                return value

        def webhook(api, event, expected=200, seconds=None):
            body = json.dumps(event).encode() if isinstance(event, dict) else event
            timestamp = str(int(time.time()) if seconds is None else seconds)
            signature = hmac.new(SECRET.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
            return request("engine", api, "/webhooks", expected, body,
                           {"X-Webhook-Timestamp": timestamp, "X-Webhook-Signature": signature})

        succeeded = False
        try:
            api = spawn("api", "serve")
            provider = spawn("provider", "provider")
            for name, base in (("engine", api), ("provider", provider)):
                assert request(name, base, "/openapi.json") == specs[name], "embedded contract differs from source"
            for path in ("/health", "/snapshot", "/inbox", "/reviews", "/metrics"):
                request("engine", api, path)
            event = dict(event_id="contract-event", invoice_id="contract-invoice", attempt_id="contract-attempt",
                         status="failed", code="insufficient_funds")
            check_value(specs["engine"], {"$ref": "#/components/schemas/Event"}, event)
            request("engine", api, "/webhooks", 401, event)
            webhook(api, event, 401, int(time.time()) - 301)
            webhook(api, b"x" * 65537, 413)
            for invalid in (b"{", dict(event, unexpected=True), dict(event, code="unknown"),
                            dict(event, status="succeeded"), dict(event, code="")):
                webhook(api, invalid, 400)
            assert webhook(api, event)["status"] == "processed"
            assert webhook(api, event)["duplicate_delivery"] is True
            assert webhook(api, dict(event, event_id="alias"))["status"] == "duplicate"
            conflict = dict(event, code="lost_card")
            assert webhook(api, conflict, 202)["status"] == "needs_review"
            assert webhook(api, conflict, 202)["duplicate_delivery"] is True
            webhook(api, dict(event_id="success", invoice_id="paid", attempt_id="paid-attempt", status="succeeded"))
            for path in ("/snapshot", "/inbox", "/reviews", "/metrics"):
                request("engine", api, path)
            recovery = dict(idempotency_key="contract-key", invoice_id="contract-invoice")
            check_value(specs["provider"], {"$ref": "#/components/schemas/RecoveryRequest"}, recovery)
            first = request("provider", provider, "/recoveries", body=recovery)
            assert request("provider", provider, "/recoveries", body=recovery) == first
            request("provider", provider, "/recoveries", 409, dict(recovery, invoice_id="other"))
            request("provider", provider, "/recoveries", 400, dict(recovery, unknown=True))
            request("provider", provider, "/recoveries", 400, b"x" * 4097)
            for key, invoice, expected in (("contract-key", "contract-invoice", 200),
                                           ("contract-key", "other", 409), ("absent", "contract-invoice", 404),
                                           ("contract-key", "", 400)):
                request("provider", provider, f"/recoveries/{key}?invoice_id={invoice}", expected,
                        route="/recoveries/{key}")
            assert request("provider", provider, "/snapshot")["provider_effects"] == 1
            for fault, code in (("rate_limit", 429), ("reject", 422), ("unavailable", 503)):
                faulty = spawn(fault, "provider", "-fault", fault)
                request("provider", faulty, "/recoveries", code, recovery)
                assert request("provider", faulty, "/snapshot")["provider_effects"] == 0
            for outcome in ("soft", "hard"):
                declining = spawn(outcome, "provider", "-outcome", outcome)
                assert request("provider", declining, "/recoveries", body=recovery)["status"] == "failed"
            succeeded = True
        finally:
            failures = []
            for process, log in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                    failures.append("service exceeded shutdown deadline")
                if process.returncode != 0:
                    failures.append(f"service exited with {process.returncode}")
                if not succeeded or process.returncode != 0:
                    log.seek(0)
                    print(log.read())
                log.close()
            if succeeded and failures:
                raise RuntimeError("; ".join(failures))
    print(json.dumps(dict(openapi="3.1.0", documents=2, live_responses=len(checked),
                          observed_operations=sorted(set(checked)), result="passed"), indent=2))


if __name__ == "__main__":
    main()
