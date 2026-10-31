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
