#!/usr/bin/env python3
import concurrent.futures
import time
import json
import urllib.request

URL = "http://127.0.0.1:5678/webhook/fde-collab"
CONCURRENCY = 20

def send_request(idx):
    payload = json.dumps({
        "fullName": f"Stress Tester {idx}",
        "email": f"tester{idx}@enterprise.local",
        "skills": "Autonomous Systems",
        "message": f"Stress testing BullMQ Queue Mode concurrency #{idx}"
    }).encode("utf-8")
    req = urllib.request.Request(URL, data=payload, headers={"Content-Type": "application/json"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            dur = time.time() - t0
            data = json.loads(resp.read().decode())
            return {"idx": idx, "status": resp.status, "dur": dur, "success": data.get("success")}
    except Exception as e:
        dur = time.time() - t0
        return {"idx": idx, "status": "ERROR", "dur": dur, "error": str(e)}

def main():
    print(f"Executing {CONCURRENCY} concurrent webhook requests against Enterprise n8n...")
    t_start = time.time()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_request, i) for i in range(CONCURRENCY)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
    total_time = time.time() - t_start

    successes = sum(1 for r in results if r.get("success") is True)
    errors = sum(1 for r in results if r.get("status") != 200)
    avg_latency = sum(r["dur"] for r in results) / len(results)

    print("=" * 60)
    print("STRESS CONCURRENCY BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Total Requests: {CONCURRENCY}")
    print(f"Successful    : {successes}/{CONCURRENCY} ({successes/CONCURRENCY*100:.1f}%)")
    print(f"Errors/Drops  : {errors}")
    print(f"Total Time    : {total_time:.2f}s")
    print(f"Avg Latency   : {avg_latency*1000:.1f}ms")
    print(f"Throughput    : {CONCURRENCY/total_time:.1f} req/s")
    print("=" * 60)

if __name__ == "__main__":
    main()
