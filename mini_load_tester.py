#!/usr/bin/env python3
import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import median
from urllib import request, error
import threading
import random
import uuid
from datetime import datetime

def parse_headers(header_list):
    headers = {}
    for h in header_list or []:
        if ":" not in h:
            print(f"Warning: ignoring malformed header '{h}'. Use 'Key: Value' format.", file=sys.stderr)
            continue
        k, v = h.split(":", 1)
        headers[k.strip()] = v.strip()
    return headers

def percentile(latencies, p):
    if not latencies:
        return None
    latencies_sorted = sorted(latencies)
    k = (len(latencies_sorted) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(latencies_sorted) - 1)
    if f == c:
        return latencies_sorted[int(k)]
    d0 = latencies_sorted[f] * (c - k)
    d1 = latencies_sorted[c] * (k - f)
    return d0 + d1

def make_payload(index: int) -> bytes:
    """
    Build a per-request JSON payload. Customize this as needed.
    Each request gets unique values so the server can see variety.
    """
    payload = {
        "request_id": index,
        "guid": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "user": {
            "id": index % 1000,
            "name": f"User{index}",
            "tier": random.choice(["free", "pro", "enterprise"]),
        },
        "metrics": {
            "score": random.randint(1, 100),
            "flags": [random.choice(["a","b","c"]), random.choice(["x","y","z"])],
        },
        "features": {
            "beta": bool(random.getrandbits(1)),
        }
    }
    return json.dumps(payload).encode("utf-8")

def do_request(idx, method, url, headers, data_bytes, timeout, start_gate: threading.Barrier, dynamic: bool):
    # Wait for all threads to be ready, then fire together
    start_gate.wait()
    start_ts = time.perf_counter()

    # Choose the right body for this request
    body = None
    if dynamic and method.upper() in ("POST","PUT","PATCH","DELETE"):
        body = make_payload(idx)
    else:
        body = data_bytes

    req = request.Request(url=url, data=body, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)

    status = None
    body_len = None
    err = None
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            rbody = resp.read()  # read to completion so latency reflects end-to-end
            body_len = len(rbody)
    except error.HTTPError as e:
        status = e.code
        try:
            rbody = e.read()
            body_len = len(rbody)
        except Exception:
            body_len = 0
        err = f"HTTPError: {e}"
    except Exception as e:
        err = f"{type(e).__name__}: {e}"

    end_ts = time.perf_counter()
    latency_ms = (end_ts - start_ts) * 1000.0
    return {
        "index": idx,
        "status": status,
        "latency_ms": latency_ms,
        "error": err,
        "response_bytes": body_len,
        "start_ts": start_ts,
        "end_ts": end_ts,
    }

def main():
    ap = argparse.ArgumentParser(description="Tiny API Load Tester (standard library only) with dynamic payload option")
    ap.add_argument("--url", required=True, help="Target URL")
    ap.add_argument("--method", default="GET", help="HTTP method (GET, POST, PUT, PATCH, DELETE)")
    ap.add_argument("--header", action="append", help="Header 'Key: Value'. Can be passed multiple times.", dest="headers")
    ap.add_argument("--data", help="Inline request body (string). Use with POST/PUT/PATCH.")
    ap.add_argument("--data-file", help="Path to file containing request body.")
    ap.add_argument("--dynamic", action="store_true", help="Generate a unique JSON payload per request (overrides --data/--data-file).")
    ap.add_argument("--concurrency", type=int, default=10, help="Number of concurrent clients (threads).")
    ap.add_argument("--requests", type=int, default=100, help="Total number of requests to send.")
    ap.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout in seconds.")
    ap.add_argument("--csv", default="results.csv", help="Path to write per-request CSV log.")
    ap.add_argument("--warmup", type=int, default=0, help="Warmup requests (not logged).")
    ap.add_argument("--content-type", help="Convenience flag to set Content-Type header (e.g. application/json).")
    args = ap.parse_args()

    if args.dynamic and args.method.upper() not in ("POST","PUT","PATCH","DELETE"):
        print("--dynamic is only useful with a method that has a body (POST/PUT/PATCH/DELETE).", file=sys.stderr)

    headers = parse_headers(args.headers)
    if args.content_type:
        headers["Content-Type"] = args.content_type
    elif args.dynamic:
        # default sensible content-type for dynamic JSON
        headers.setdefault("Content-Type", "application/json")

    if args.data_file and args.data:
        print("Specify only one of --data or --data-file.", file=sys.stderr)
        sys.exit(2)

    data_bytes = None
    if not args.dynamic:
        if args.data_file:
            with open(args.data_file, "rb") as f:
                data_bytes = f.read()
        elif args.data is not None:
            data_bytes = args.data.encode("utf-8")

    # Warmup (optional)
    if args.warmup > 0:
        print(f"Warmup: sending {args.warmup} request(s)...", file=sys.stderr)
        start_gate = threading.Barrier(min(args.concurrency, args.warmup) if args.warmup > 0 else 1)
        with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futures = []
            for i in range(args.warmup):
                futures.append(ex.submit(do_request, i, args.method, args.url, headers, data_bytes, args.timeout, start_gate, args.dynamic))
            for fut in as_completed(futures):
                _ = fut.result()
        print("Warmup complete.", file=sys.stderr)

    # Main run
    n = args.requests
    conc = min(args.concurrency, n) if n > 0 else args.concurrency
    start_gate = threading.Barrier(conc if conc > 0 else 1)
    print(f"Starting run: {n} requests, concurrency {conc}", file=sys.stderr)
    results = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as ex:
        futures = []
        # Submit in batches of 'conc' to better approximate bursts of parallel hits.
        submitted = 0
        while submitted < n:
            batch = min(conc, n - submitted)
            # new barrier for each batch so each group fires at (roughly) the same time
            start_gate = threading.Barrier(batch)
            for i in range(submitted, submitted + batch):
                futures.append(ex.submit(do_request, i, args.method, args.url, headers, data_bytes, args.timeout, start_gate, args.dynamic))
            submitted += batch

        for fut in as_completed(futures):
            results.append(fut.result())
    t1 = time.perf_counter()

    # Write CSV
    with open(args.csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["index", "status", "latency_ms", "response_bytes", "error"])
        for r in sorted(results, key=lambda x: x["index"]):
            w.writerow([r["index"], r["status"], f"{r['latency_ms']:.2f}", r.get("response_bytes") or 0, r["error"] or ""])

    # Aggregate
    latencies = [r["latency_ms"] for r in results if r["latency_ms"] is not None]
    successes = [r for r in results if (r["status"] is not None and 200 <= r["status"] < 400)]
    failures = [r for r in results if not (r["status"] is not None and 200 <= r["status"] < 400)]
    total = len(results)
    wall_secs = (t1 - t0)
    success_rate = (len(successes) / total * 100.0) if total else 0.0
    rps = (total / wall_secs) if wall_secs > 0 else 0.0

    p50 = median(latencies) if latencies else None
    p90 = percentile(latencies, 90) if latencies else None
    p95 = percentile(latencies, 95) if latencies else None
    p99 = percentile(latencies, 99) if latencies else None

    print("\n=== Summary ===")
    print(f"Target: {args.url}")
    print(f"Method: {args.method}")
    if args.dynamic:
        print("Payload: dynamic JSON per request")
    elif data_bytes is not None:
        print(f"Payload bytes: {len(data_bytes)}")
    print(f"Requests: {total}")
    print(f"Concurrency: {conc}")
    print(f"Wall time: {wall_secs:.2f}s")
    print(f"Throughput: {rps:.2f} req/s")
    print(f"Success rate: {success_rate:.2f}% ({len(successes)}/{total})")
    if latencies:
        print("Latency (ms):")
        print(f"  p50: {p50:.2f}")
        print(f"  p90: {p90:.2f}")
        print(f"  p95: {p95:.2f}")
        print(f"  p99: {p99:.2f}")
        print(f"  max: {max(latencies):.2f}")
        print(f"  min: {min(latencies):.2f}")
    print(f"\nPer-request log written to: {args.csv}")
    if failures:
        print(f"Failures: {len(failures)} (see CSV for details)")

if __name__ == "__main__":
    main()
