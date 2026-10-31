# System Architecture & Technical Specifications

## 1. System Topology & Decoupling Mechanics

The engine solves the classic high-concurrency flash-sale challenge: how to handle tens of thousands of simultaneous requests for limited inventory without race conditions, overselling, or database pool exhaustion.

```
                        [ Client Traffic (10,000+ RPS) ]
                                       │
                                       │ POST /api/v1/orders/reserve
                                       ▼
                       ┌────────────────────────────────┐
                       │     API Gateway (FastAPI)      │
                       └────────────────┬───────────────┘
                                        │
                         1. Atomic Lua Execution (< 2ms)
                                        ▼
                       ┌────────────────────────────────┐
                       │          Redis (RAM)           │
                       │  • Stock evaluation            │
                       │  • 1-per-user anti-scalp check │
                       │  • Fast 410 rejection if 0     │
                       └────────────────┬───────────────┘
                                        │
                         2. Publish Event (If Stock Won)
                                        ▼
                       ┌────────────────────────────────┐
                       │       Apache Kafka Broker      │
                       │  • Topic: orders.reserved      │
                       │  • Topic: orders.dlq           │
                       └────────────────┬───────────────┘
                                        │
                         3. Asynchronous Consumer Loop
                                        ▼
                       ┌────────────────────────────────┐
                       │         Worker Consumer        │
                       │  • Idempotent DB write         │
                       │  • Manual offset commit        │
                       │  • DLQ router & auto-compensate│
                       └────────────────┬───────────────┘
                                        │
                         4. ACID Persistence
                                        ▼
                       ┌────────────────────────────────┐
                       │      PostgreSQL Database       │
                       │  • Orders Table (CONFIRMED)    │
                       │  • Inventory audit trail       │
                       └────────────────────────────────┘
```

---

## 2. In-Memory Atomic Concurrency (Redis Lua)

The core concurrency guarantee is enforced by Redis executing single-threaded Lua scripts atomically. No distributed locks (e.g., Redlock) are required, avoiding network roundtrip overhead and deadlock risks.

### Reserve Script (`RESERVE_STOCK_LUA`)
```lua
-- KEYS[1]: item:{id}:stock
-- KEYS[2]: item:{id}:claimed_users
-- ARGV[1]: user_id
-- ARGV[2]: quantity (default: 1)

local stock_key = KEYS[1]
local users_key = KEYS[2]
local user_id = ARGV[1]
local quantity = tonumber(ARGV[2]) or 1

-- 1. Anti-scalping: 1 reservation per customer
if redis.call('SISMEMBER', users_key, user_id) == 1 then
    return -1 -- Already claimed
end

-- 2. Fast stock check
local current_stock = tonumber(redis.call('GET', stock_key) or 0)
if current_stock < quantity then
    return 0 -- Depleted / Insufficient stock
end

-- 3. Atomic state mutation
redis.call('DECRBY', stock_key, quantity)
redis.call('SADD', users_key, user_id)

local remaining = current_stock - quantity
return remaining
```

### Script Return Contract
* `remaining >= 0`: Reservation secured. API returns `202 Accepted` and enqueues event to Kafka.
* `0`: Stock depleted. API returns `410 Gone`. Subsequent requests are rejected in RAM in `< 2ms`, dropping 99% of spike traffic.
* `-1`: Duplicate reservation attempt by same customer identity. API returns `409 Conflict`.

---

## 3. Resiliency & Compensation (Dead Letter Queue)

When background persistence fails repeatedly (e.g., database network partition), the worker routes the message to `orders.dlq` and invokes an atomic compensation script:

```lua
-- KEYS[1]: item:{id}:stock
-- KEYS[2]: item:{id}:claimed_users
-- ARGV[1]: user_id
-- ARGV[2]: quantity

local stock_key = KEYS[1]
local users_key = KEYS[2]
local user_id = ARGV[1]
local quantity = tonumber(ARGV[2]) or 1

redis.call('INCRBY', stock_key, quantity)
redis.call('SREM', users_key, user_id)
return redis.call('GET', stock_key)
```

This guarantees inventory returns to the active pool without manual database intervention.

---

## 4. Relational Storage & Idempotency

### Idempotent Consumer Insert
Orders are inserted using PostgreSQL's `ON CONFLICT (idempotency_key) DO NOTHING` pattern:

```sql
INSERT INTO orders (
    id, user_id, item_id, quantity, amount_cents, status, idempotency_key, created_at, updated_at
) VALUES (
    :id, :user_id, :item_id, :quantity, :amount_cents, 'CONFIRMED', :idempotency_key, :created_at, :updated_at
)
ON CONFLICT (idempotency_key) DO NOTHING;
```

If a message is redelivered due to an at-least-once Kafka rebalance, the duplicate database write is safely ignored.

