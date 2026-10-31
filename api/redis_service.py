import json
import logging
from typing import Optional
import redis.asyncio as aioredis
from shared.config import settings
from shared.constants import (
    RESERVE_STOCK_LUA,
    COMPENSATE_STOCK_LUA,
    LuaReturnCode,
    key_item_stock,
    key_item_buyers,
    key_item_updates_channel,
)

logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self):
        self.client = None
        self._reserve_script = None
        self._compensate_script = None
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

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except Exception:
            return False

redis_service = RedisService()
