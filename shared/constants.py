from enum import IntEnum

class LuaReturnCode(IntEnum):
    SUCCESS = 1
    SOLD_OUT = 0
    ALREADY_PURCHASED = -1

TOPIC_ORDERS_RESERVED = "orders.reserved"
TOPIC_ORDERS_DLQ = "orders.dlq"

def key_item_stock(item_id: str) -> str:
    return f"item:{item_id}:stock"

def key_item_buyers(item_id: str) -> str:
    return f"item:{item_id}:buyers"

def key_item_updates_channel(item_id: str) -> str:
    return f"channel:item:{item_id}:updates"

def key_idempotency(key: str) -> str:
    return f"order:idem:{key}"

def key_rate_limit(user_id: str) -> str:
    return f"rate:user:{user_id}"

KEY_CATALOG_CACHE = "cache:catalog:items"
CACHE_CATALOG_TTL_SECONDS = 30

# RabbitMQ Topology Constants
RABBITMQ_EXCHANGE_EVENTS = "flash_sale.events"
RABBITMQ_QUEUE_RESERVED = "orders.reserved"
RABBITMQ_ROUTING_KEY_RESERVED = "order.reserved"
RABBITMQ_EXCHANGE_DLX = "flash_sale.dlx"
RABBITMQ_QUEUE_DLQ = "orders.dlq"
RABBITMQ_ROUTING_KEY_DLQ = "order.dlq"
RABBITMQ_QUEUE_RETRY = "orders.retry"

# Sliding Window Rate Limiting Lua Script
# KEYS[1]: rate limit key
# ARGV[1]: current timestamp in milliseconds
# ARGV[2]: window size in milliseconds
# ARGV[3]: max requests allowed in window
# Returns: 1 if allowed, 0 if rate limit exceeded
SLIDING_WINDOW_RATE_LIMIT_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local clear_before = now - window
redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

local current_requests = redis.call('ZCARD', key)
if current_requests < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(1000, 9999))
    redis.call('PEXPIRE', key, window)
    return 1
else
    return 0
end
"""

# Atomic check-and-decrement script executed inside Redis event loop
RESERVE_STOCK_LUA = """
local stock_key = KEYS[1]
local buyers_key = KEYS[2]
local user_id = ARGV[1]

local already_bought = redis.call('SISMEMBER', buyers_key, user_id)
if already_bought == 1 then
    return -1
end

local current_stock = tonumber(redis.call('GET', stock_key))
if not current_stock or current_stock <= 0 then
    return 0
end

redis.call('DECR', stock_key)
redis.call('SADD', buyers_key, user_id)
return 1
"""

# Compensation script restoring stock upon persistence failure
COMPENSATE_STOCK_LUA = """
local stock_key = KEYS[1]
local buyers_key = KEYS[2]
local user_id = ARGV[1]

redis.call('INCR', stock_key)
redis.call('SREM', buyers_key, user_id)
return 1
"""
