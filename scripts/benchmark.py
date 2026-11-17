#!/usr/bin/env python3
"""Measure the compiled synthetic HTTP stack; never connect to a real provider."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import platform
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SECRET = "disposable-benchmark-secret"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def percentile(values, percent):
    return sorted(values)[max(0, math.ceil(len(values) * percent / 100) - 1)]


def scenario(binary, invoices, copies, concurrency, workers, run):
    with tempfile.TemporaryDirectory(prefix="recovery-benchmark-") as directory:
        root = Path(directory)
        api_port, provider_port = port(), port()
        while api_port == provider_port:
            provider_port = port()
        api = f"http://127.0.0.1:{api_port}"
        provider = f"http://127.0.0.1:{provider_port}"
        engine_db = root / "engine.db"
        processes = []

        def spawn(name, *args):
            log = (root / (name + ".log")).open("w+")
            try:
                process = subprocess.Popen([str(binary), *args], stdout=log, stderr=log,
                                           env=dict(os.environ, RECOVERY_WEBHOOK_SECRET=SECRET))
            except BaseException:
                log.close()
                raise
            processes.append((name, process, log))

        def wait_for(url, predicate, timeout):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if any(process.poll() is not None for _, process, _ in processes):
                    raise RuntimeError("a benchmark service exited unexpectedly")
                try:
                    value = get(url)
                    if predicate(value):
                        return value
                except OSError:
                    pass
                time.sleep(0.05)
            raise TimeoutError(f"scenario did not finish within {timeout} seconds")

        def send(index):
            body = json.dumps(dict(event_id=f"delivery-{index}", invoice_id=f"invoice-{index}",
                                   attempt_id=f"attempt-{index}", status="failed",
                                   code="insufficient_funds")).encode()
            timestamp = str(int(time.time()))
            signature = hmac.new(SECRET.encode(), timestamp.encode() + b"." + body,
                                 hashlib.sha256).hexdigest()
            request = urllib.request.Request(api + "/webhooks", body, headers={
                "Content-Type": "application/json", "X-Webhook-Timestamp": timestamp,
                "X-Webhook-Signature": signature,
            })
            started = time.perf_counter()
            with urllib.request.urlopen(request, timeout=15) as response:
                result = json.load(response)
                if response.status != 200 or result["status"] != "processed":
                    raise RuntimeError(f"unexpected webhook response: {result}")
            return (time.perf_counter() - started) * 1000, result["duplicate_delivery"]

        succeeded = False
        try:
            spawn("provider", "provider", "-db", str(root / "provider.db"),
                  "-listen", f"127.0.0.1:{provider_port}")
            spawn("api", "serve", "-db", str(engine_db), "-listen", f"127.0.0.1:{api_port}")
            wait_for(api + "/health", lambda v: v["status"] == "ok", 10)
            wait_for(provider + "/snapshot", lambda v: v["provider_effects"] == 0, 10)
            started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                responses = list(pool.map(send, [i for i in range(invoices) for _ in range(copies)]))
            ingestion_seconds = time.perf_counter() - started
            latencies = [latency for latency, _ in responses]
            redeliveries = sum(duplicate for _, duplicate in responses)
            if redeliveries != invoices * (copies - 1):
                raise RuntimeError("delivery deduplication invariant failed")
            # Only this disposable fixture is advanced, before any worker starts.
            # Runtime backoff and the CLI worker's 250 ms poll interval stay intact.
            with sqlite3.connect(engine_db) as connection:
                connection.execute("UPDATE jobs SET due_at=0 WHERE state='queued'")
            started = time.perf_counter()
            for index in range(workers):
                spawn(f"worker-{index}", "worker", "-db", str(engine_db), "-gateway", provider)
            final = wait_for(api + "/snapshot", lambda v: v["pending_jobs"] == 0,
                             max(30, math.ceil(invoices / workers) + 15))
            recovery_seconds = time.perf_counter() - started
            effects = get(provider + "/snapshot")["provider_effects"]
            if (len(final["invoices"]) != invoices or
                    any(v["state"] != "paid" for v in final["invoices"]) or
                    final["payment_attempts"] != invoices * 2 or effects != invoices or
                    final["retry_jobs"] != invoices or final["review_jobs"] != 0):
                raise RuntimeError(f"recovery invariants failed: {final}; provider effects={effects}")
            succeeded = True
            return dict(run=run, workers=workers, invoices=invoices, webhook_requests=len(responses),
                        redeliveries=redeliveries,
                        ingestion_seconds=round(ingestion_seconds, 6),
                        webhook_requests_per_second=round(len(responses) / ingestion_seconds, 3),
                        webhook_latency_ms=dict(p50=round(percentile(latencies, 50), 3),
                                                p95=round(percentile(latencies, 95), 3),
                                                maximum=round(max(latencies), 3)),
                        recovery_seconds=round(recovery_seconds, 6),
                        recovered_invoices_per_second=round(invoices / recovery_seconds, 3),
                        provider_effects=effects, payment_attempts=final["payment_attempts"],
                        pending_jobs=final["pending_jobs"], invariants="passed")
        finally:
            failures = []
            for name, process, log in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                    failures.append(name + " exceeded shutdown deadline")
                if process.returncode != 0:
                    failures.append(f"{name} exited with {process.returncode}")
                if not succeeded or process.returncode != 0:
                    log.seek(0)
                    print(f"{name}: {log.read()}", file=sys.stderr)
                log.close()
            if succeeded and failures:
                raise RuntimeError("; ".join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invoices", type=int, default=40)
    parser.add_argument("--copies", type=int, default=3, help="total deliveries per invoice")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--workers", default="1,4", help="comma-separated worker counts")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        workers = [int(value) for value in args.workers.split(",")]
    except ValueError:
        parser.error("--workers requires comma-separated positive integers")
    if min(args.invoices, args.copies, args.concurrency, args.runs, *workers) < 1:
        parser.error("all counts must be positive")
    binary = ROOT / "bin" / "recovery"
    if not binary.is_file():
        parser.error("build first: go build -o bin/recovery ./cmd/recovery")
    started = datetime.now(timezone.utc).isoformat()
    samples = [scenario(binary, args.invoices, args.copies, args.concurrency, count, run)
               for run in range(1, args.runs + 1) for count in workers]
    metadata = dict(os=platform.platform(), python=platform.python_version(),
                    logical_cpus=os.cpu_count(),
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    git_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                    working_tree_dirty=bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)))
    for name, path in {"cpu_quota": "/sys/fs/cgroup/cpu.max", "memory_limit": "/sys/fs/cgroup/memory.max"}.items():
        if Path(path).exists():
            metadata[name] = Path(path).read_text().strip()
    if Path("/proc/cpuinfo").exists():
        metadata["cpu_model"] = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                                      if line.startswith("model name")), "unknown")
    report = dict(synthetic=True, started_at_utc=started, environment=metadata,
                  methodology="loopback HTTP; fresh SQLite files per run; initial due times advanced; CLI workers unchanged; startup and 50 ms completion polling included",
                  configuration=dict(invoices=args.invoices, deliveries_per_invoice=args.copies,
                                     client_concurrency=args.concurrency, workers=workers, runs=args.runs),
                  samples=samples,
                  median_recovered_invoices_per_second={str(count): statistics.median(
                      s["recovered_invoices_per_second"] for s in samples if s["workers"] == count)
                      for count in workers})
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
