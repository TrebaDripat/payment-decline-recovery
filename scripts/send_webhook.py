#!/usr/bin/env python3
"""Send a signed synthetic webhook to the loopback demo. No payment provider calls."""
import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--event-id", default="delivery-1")
parser.add_argument("--invoice-id", default="synthetic-invoice")
parser.add_argument("--attempt-id", default="initial-attempt")
parser.add_argument("--port", type=int, default=8080)
args = parser.parse_args()
secret = os.environ["RECOVERY_WEBHOOK_SECRET"]
body = json.dumps({"event_id": args.event_id, "invoice_id": args.invoice_id,
                   "attempt_id": args.attempt_id, "status": "failed",
                   "code": "insufficient_funds"}).encode()
timestamp = str(int(time.time()))
signature = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
request = urllib.request.Request(f"http://127.0.0.1:{args.port}/webhooks", body, headers={
    "Content-Type": "application/json", "X-Webhook-Timestamp": timestamp,
    "X-Webhook-Signature": signature,
})
with urllib.request.urlopen(request, timeout=10) as response:
    print(response.read().decode().strip())
