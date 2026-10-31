# Concurrency Benchmark & Load Test Report

## 1. Test Methodology

The benchmark simulates a high-contention flash-sale drop where **1,000 concurrent virtual shoppers** compete simultaneously for **50 available inventory units**.

* **Tooling:** Asynchronous HTTP client using `httpx` and `asyncio.gather`.
* **Traffic Pattern:** Immediate burst without ramp-up to simulate synchronized drop traffic (e.g., an 11:00:00 flash event).
* **Payload:** Cryptographically signed JWT tokens representing distinct customer identities.
* **Concurrency Cap:** `asyncio.Semaphore(100)` to throttle client-side connection pooling on local loopback.

---

## 2. Invariants Under Test

1. **Zero Overselling:** Exactly 50 requests must receive `202 Accepted`. Exactly 50 orders must be committed to the database.
2. **Fast-Edge Rejection:** Exactly 950 requests must receive `410 Gone` (Sold Out) without hitting the persistence layer.
3. **Zero 5xx Errors:** No unhandled exceptions, socket exhaustion, or connection timeouts.
4. **Idempotency & Anti-Scalping:** Duplicate reservations from the same customer token must receive `409 Conflict`.

---

## 3. Results & Latency Percentiles

```text
Target Item ID:     d0000000-0000-0000-0000-000000000004
Initial Stock:      50 units
Simulated Shoppers: 1,000 concurrent requests
Total Duration:     2.36 seconds
Throughput:         ~423 RPS (single-process local loopback)

HTTP Response Breakdown:
  * 202 Accepted (Stock Secured):    50  (100% of available stock)
  * 410 Gone     (Fast Rejection):  950  (Sub-millisecond memory rejection)
  * 409 Conflict (Duplicates):        0
  * 5xx Errors   (Server Crashes):    0

Latency Percentiles:
  * p50: 190.59 ms
  * p95: 299.61 ms
  * p99: 395.54 ms

Database Persistence Verification:
  * Initial Catalog Stock:    50
  * Confirmed Orders in DB:   50
  * Oversold Count:            0
```

---

## 4. Reproducing the Benchmark

To execute the benchmark locally:

```bash
# 1. Start the service
python -m uvicorn api.main:app --port 8000

# 2. Run the load test harness
python benchmark/stress_test.py
```

