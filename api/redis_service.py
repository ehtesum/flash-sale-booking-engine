import time
import json
import logging
from typing import Optional, Any
import redis.asyncio as aioredis
from shared.config import settings
from shared.constants import (
    RESERVE_STOCK_LUA,
    COMPENSATE_STOCK_LUA,
    SLIDING_WINDOW_RATE_LIMIT_LUA,
    KEY_CATALOG_CACHE,
    CACHE_CATALOG_TTL_SECONDS,
    LuaReturnCode,
    key_item_stock,
    key_item_buyers,
    key_item_updates_channel,
    key_rate_limit,
)

logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self):
        self.client = None
        self._reserve_script = None
        self._compensate_script = None
        self._rate_limit_script = None
        self.is_fake = False

    async def connect(self):
        if settings.STANDALONE_MODE:
            logger.info("[Redis] Operating in Standalone Zero-Docker mode (in-memory FakeRedis with Lua engine).")
            import fakeredis.aioredis
            self.client = fakeredis.aioredis.FakeRedis(decode_responses=True)
            self.is_fake = True
        else:
            try:
                self.client = aioredis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    socket_timeout=2.0,
                    socket_connect_timeout=2.0,
                )
                await self.client.ping()
                logger.info("[Redis] Connected to live Redis instance.")
            except Exception as e:
                logger.warning(f"[Redis] Real Redis unreachable ({e}). Falling back to in-memory FakeRedis.")
                import fakeredis.aioredis
                self.client = fakeredis.aioredis.FakeRedis(decode_responses=True)
                self.is_fake = True

        # Register Lua scripts
        self._reserve_script = self.client.register_script(RESERVE_STOCK_LUA)
        self._compensate_script = self.client.register_script(COMPENSATE_STOCK_LUA)
        self._rate_limit_script = self.client.register_script(SLIDING_WINDOW_RATE_LIMIT_LUA)

    async def close(self):
        if self.client:
            await self.client.close()

    async def reserve_stock(self, item_id: str, user_id: str) -> LuaReturnCode:
        """Atomically checks and decrements stock in Redis (< 2ms)."""
        stock_key = key_item_stock(item_id)
        buyers_key = key_item_buyers(item_id)
        
        result = await self._reserve_script(
            keys=[stock_key, buyers_key],
            args=[user_id]
        )
        return LuaReturnCode(int(result))

    async def compensate_stock(self, item_id: str, user_id: str) -> bool:
        """Atomically restores stock back into pool if downstream write fails."""
        stock_key = key_item_stock(item_id)
        buyers_key = key_item_buyers(item_id)

        result = await self._compensate_script(
            keys=[stock_key, buyers_key],
            args=[user_id]
        )
        return int(result) == 1

    async def get_stock(self, item_id: str) -> int:
        """Returns the current available in-memory stock."""
        val = await self.client.get(key_item_stock(item_id))
        return int(val) if val is not None else 0

    async def set_stock(self, item_id: str, stock: int):
        """Sets inventory count in Redis (pre-warming)."""
        await self.client.set(key_item_stock(item_id), stock)
        await self.client.delete(key_item_buyers(item_id))

    async def publish_stock_update(self, item_id: str, stock: int):
        """Publishes real-time stock change to SSE channel."""
        channel = key_item_updates_channel(item_id)
        payload = json.dumps({"item_id": item_id, "stock": stock})
        await self.client.publish(channel, payload)

    async def check_rate_limit(
        self,
        identifier: str,
        limit: int = settings.RATE_LIMIT_REQUESTS,
        window_seconds: int = settings.RATE_LIMIT_WINDOW_SECONDS
    ) -> bool:
        """Sliding-window rate limiter evaluated atomically in Redis. Returns True if allowed, False if exceeded."""
        if not self.client:
            return True  # Fail-open gracefully if cache is uninitialized
        try:
            now_ms = int(time.time() * 1000)
            window_ms = window_seconds * 1000
            key = key_rate_limit(identifier)

            # In fakeredis without registered script support or standard lua
            if self.is_fake:
                # Direct sorted set commands fallback for fake redis environments
                clear_before = now_ms - window_ms
                await self.client.zremrangebyscore(key, 0, clear_before)
                current = await self.client.zcard(key)
                if current < limit:
                    await self.client.zadd(key, {f"{now_ms}": now_ms})
                    await self.client.pexpire(key, window_ms)
                    return True
                return False

            result = await self._rate_limit_script(
                keys=[key],
                args=[now_ms, window_ms, limit]
            )
            return int(result) == 1
        except Exception as e:
            logger.warning(f"[RateLimiter] Error evaluating rate limit: {e}. Failing open.")
            return True

    async def get_cached_catalog(self) -> Optional[list[dict]]:
        """Cache-aside pattern: retrieves cached product catalog from Redis."""
        if not self.client:
            return None
        try:
            data = await self.client.get(KEY_CATALOG_CACHE)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning(f"[Cache] Catalog cache read error: {e}")
        return None

    async def set_cached_catalog(self, catalog: list[dict], ttl: int = CACHE_CATALOG_TTL_SECONDS):
        """Sets cached catalog with short TTL to reduce PostgreSQL query load."""
        if not self.client:
            return
        try:
            await self.client.set(
                KEY_CATALOG_CACHE,
                json.dumps(catalog, default=str),
                ex=ttl
            )
        except Exception as e:
            logger.warning(f"[Cache] Catalog cache write error: {e}")

    async def invalidate_catalog_cache(self):
        """Invalidates catalog cache upon inventory changes or administrative resets."""
        if not self.client:
            return
        try:
            await self.client.delete(KEY_CATALOG_CACHE)
        except Exception as e:
            logger.warning(f"[Cache] Catalog cache invalidation error: {e}")

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except Exception:
            return False

redis_service = RedisService()
