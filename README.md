# Flash-Sale & Distributed Booking Engine

[![CI](https://github.com/ehtesum/flash-sale-booking-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/ehtesum/flash-sale-booking-engine/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A high-concurrency reservation engine designed to eliminate race conditions and prevent inventory overselling during traffic bursts (e.g. e-commerce flash sales, limited hotel drops, seat reservations).

The architecture decouples sub-millisecond in-memory inventory decrement (single-threaded Redis Lua) from asynchronous relational database persistence (Apache Kafka event stream into PostgreSQL).

---

## Architecture Overview

```text
                        [ Concurrent Client Traffic (10k+ RPS) ]
                                          │
                                          │ POST /api/v1/orders/reserve
                                          ▼
                      ┌───────────────────────────────────────┐
                      │          API Gateway (FastAPI)        │
                      └───────────────────┬───────────────────┘
                                          │
                        1. Atomic Lua Check & Decrement (< 2ms)
                                          ▼
                      ┌───────────────────────────────────────┐
                      │               Redis (RAM)             │
                      │  • Evaluates available stock          │
                      │  • Enforces 1-per-user constraint     │
                      │  • Fast 410 rejection if stock == 0   │
                      └───────────────────┬───────────────────┘
                                          │
                        2. Publish Event (If Stock Secured)
                                          ▼
                      ┌───────────────────────────────────────┐
                      │          Apache Kafka Broker          │
                      │  • Topic: orders.reserved             │
                      │  • Topic: orders.dlq (Dead Letter)    │
                      └───────────────────┬───────────────────┘
                                          │
                        3. Asynchronous Consumer Loop
                                          ▼
                      ┌───────────────────────────────────────┐
                      │        Worker Service (Consumer)      │
                      │  • Manual offset commit after DB write│
                      │  • DLQ router & auto-compensator      │
                      └───────────────────┬───────────────────┘
                                          │
                        4. Transactional Commit
                                          ▼
                      ┌───────────────────────────────────────┐
                      │         PostgreSQL Database           │
                      │  • Orders Table (Status: CONFIRMED)   │
                      │  • Append-only inventory audit log    │
                      └───────────────────────────────────────┘
```

---

## Core Invariants

* **Zero Overselling:** Stock is decremented in memory using an atomic Lua script (`RESERVE_STOCK_LUA`). Because Redis executes Lua scripts serially in a single-threaded event loop, race conditions between concurrent requests are prevented without distributed lock overhead.
* **Fast Edge Rejection:** When inventory drops to 0, subsequent requests are rejected at the Redis layer in $< 2\text{ms}$ with HTTP `410 Gone`, dropping $99\%$ of spike traffic before it hits message brokers or storage.
* **Asynchronous Decoupling:** Client requests return HTTP `202 Accepted` immediately upon in-memory reservation. Database writes are buffered through Kafka, protecting database connection pools from exhaustion.
* **Self-Healing Compensation:** If worker persistence fails repeatedly, messages route to `orders.dlq` and an atomic compensation script runs `INCRBY` to restore inventory to the active pool.
* **Stateless Authentication:** Evaluates JWT cryptographic signatures in memory with zero database roundtrips during sale spikes.

---

## Project Structure

```text
flash_sale_booking_engine/
├── api/                        # Producer Gateway
│   ├── main.py                 # Application lifecycle & middleware
│   ├── routes.py               # REST API endpoints & SSE stream
│   ├── auth.py                 # Stateless JWT verification
│   ├── redis_service.py        # Atomic Lua scripts & pub/sub
│   ├── kafka_service.py        # Event stream producer
│   ├── db.py                   # Catalog read queries
│   └── static/                 # Storefront & SRE Ops dashboard
│       ├── index.html          # Semantic HTML layout
│       ├── css/styles.css      # E-commerce styles
│       └── js/app.js           # SSE client, timer & test runner
├── worker/                     # Persistence Consumer
│   ├── main.py                 # Consumer loop with manual commit
│   ├── db.py                   # ACID transaction persistence
│   └── compensator.py          # DLQ router & stock compensator
├── shared/                     # Single source of truth
│   ├── config.py               # Pydantic settings singleton
│   ├── constants.py            # Redis keys, topic names & Lua scripts
│   ├── database.py             # Shared async database engine & pool
│   └── schemas.py              # Pydantic models & RFC 7807 errors
├── benchmark/                  # Load testing harness
│   └── stress_test.py          # 1,000-user concurrency test
├── tests/                      # Integration verification
│   └── smoke_test.py           # End-to-end full-stack smoke test
├── migrations/                 # Relational DDL
│   └── 001_init_schema.sql     # PostgreSQL tables, indexes & seed data
├── docs/                       # Engineering specifications
│   ├── architecture.md         # Detailed topology & failure modes
│   └── benchmarks.md           # Concurrency methodology & latency
├── docker-compose.yml          # Multi-container cluster orchestration
├── Makefile                    # Developer targets
└── requirements.txt            # Python dependencies
```

---

## Getting Started

### Prerequisites
* Python 3.11+
* Docker & Docker Compose (optional for local developer mode)

### Environment 1: High-Velocity Local Developer Mode
The project features a zero-dependency local developer mode utilizing `fakeredis` (with native C-Lua execution via `lupa`) and SQLite. This enables instant local boot and sub-second test runs without container overhead:

1. Install dependencies:
   ```bash
   pip install -r requirements.txt fakeredis lupa aiosqlite
   ```

2. Start the API Gateway:
   ```bash
   python -m uvicorn api.main:app --port 8000
   ```

3. Access the interfaces:
   * **Storefront & Ops Dashboard:** [http://localhost:8000](http://localhost:8000)
   * **Interactive OpenAPI Specs:** [http://localhost:8000/docs](http://localhost:8000/docs)

### Environment 2: Multi-Container Production Topology
Runs PostgreSQL 15, Redis 7 (AOF enabled), Apache Kafka 7.5.0, ZooKeeper, API Gateway, and background consumers in isolated containers:

```bash
docker compose up -d --build
```

---

## Verification & Benchmarks

### 1. Full-Stack Smoke Test
Validates health probes, JWT issuance, catalog querying, reservation, duplicate order prevention (HTTP 409), and database commit:
```bash
python tests/smoke_test.py
```

### 2. 1,000-User Concurrency Benchmark
Fires 1,000 concurrent asynchronous requests competing for 50 inventory units:
```bash
python benchmark/stress_test.py
```

```text
=================================================================
   DISTRIBUTED FLASH-SALE ENGINE: 1,000-USER STRESS BENCHMARK
=================================================================
Target Item ID:     d0000000-0000-0000-0000-000000000004
Initial Stock:      50 units
Simulated Shoppers: 1000 concurrent virtual users
-----------------------------------------------------------------
Dispatching 1,000 concurrent asynchronous requests...
All 1000 requests completed in 2.36 seconds.
-----------------------------------------------------------------
HTTP RESPONSE BREAKDOWN:
  * 202 Accepted (Stock Secured):     50  (Expected: 50)
  * 410 Gone     (Sold Out):         950  (Expected: 950)
  * 409 Conflict (Duplicates):         0
  * 5xx Errors   (Crashes):            0
-----------------------------------------------------------------
LATENCY PERCENTILES:
  * p50: 190.59 ms
  * p95: 299.61 ms
  * p99: 395.54 ms
-----------------------------------------------------------------
DATABASE PERSISTENCE VERIFICATION:
  * Initial Catalog Stock:         50
  * Confirmed Orders in DB:        50
  * OVERSOLD COUNT:                0
=================================================================
  [PASS] Concurrency verification: 50 orders committed, 0 oversold, 0 server errors.
=================================================================
```

---

## API Reference

| Method | Endpoint | Description | Status Codes |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/v1/orders/reserve` | Atomic inventory reservation | `202`, `409`, `410` |
| `GET` | `/api/v1/orders/{order_id}` | Order persistence status query | `200`, `404` |
| `GET` | `/api/v1/items` | Catalog list with live Redis stock | `200` |
| `GET` | `/api/v1/items/{item_id}/stream` | Server-Sent Events (SSE) stock stream | `200` |
| `POST` | `/api/v1/auth/demo-token` | Generates signed demo JWT token | `200`, `400` |
| `POST` | `/api/v1/admin/reset-stock` | Resets item stock and clears orders | `200` |
| `GET` | `/healthz` | Liveness health probe | `200` |
| `GET` | `/ready` | Readiness probe (Redis, Kafka, DB) | `200`, `503` |

---

## Documentation

* [`docs/architecture.md`](docs/architecture.md): Concurrency model, Redis Lua scripts, failure modes, and PostgreSQL idempotency.
* [`docs/benchmarks.md`](docs/benchmarks.md): Concurrency test methodology, latency percentiles, and hardware baseline.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
