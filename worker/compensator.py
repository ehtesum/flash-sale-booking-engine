import json
import logging
from datetime import datetime, timezone
import redis.asyncio as aioredis
from shared.config import settings
from shared.constants import (
    TOPIC_ORDERS_DLQ,
    COMPENSATE_STOCK_LUA,
    key_item_stock,
    key_item_buyers,
    key_item_updates_channel,
)
from shared.schemas import OrderFailedEvent

logger = logging.getLogger(__name__)

class WorkerCompensator:
    def __init__(self):
        self.redis_client = None
        self.kafka_producer = None
        self._compensate_script = None

    async def start(self):
        if settings.STANDALONE_MODE:
            import fakeredis.aioredis
            self.redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        else:
            self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

        self._compensate_script = self.redis_client.register_script(COMPENSATE_STOCK_LUA)

        if not settings.STANDALONE_MODE:
            try:
                from aiokafka import AIOKafkaProducer
                self.kafka_producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                )
                await self.kafka_producer.start()
            except Exception as e:
                logger.warning(f"Kafka producer unavailable in compensator: {e}")

    async def close(self):
        if self.kafka_producer:
            await self.kafka_producer.stop()
        if self.redis_client:
            await self.redis_client.close()

    async def handle_failure(self, event_data: dict, reason: str, error_details: dict | None = None):
        """Routes failed events to DLQ and triggers in-memory stock compensation."""
        order_id = event_data.get("order_id")
        item_id = str(event_data.get("item_id"))
        user_id = str(event_data.get("user_id"))
        correlation_id = event_data.get("correlation_id", "unknown")

        logger.warning(f"[DLQ] Diverting order {order_id} to DLQ. Reason: {reason}")

        dlq_event = OrderFailedEvent(
            order_id=order_id,
            item_id=item_id,
            user_id=user_id,
            reason=reason,
            error_details=error_details,
            correlation_id=correlation_id,
            failed_at=datetime.now(timezone.utc),
        )

        if self.kafka_producer:
            await self.kafka_producer.send_and_wait(
                TOPIC_ORDERS_DLQ,
                value=dlq_event.model_dump(mode="json")
            )

        # Restore stock in Redis
        stock_key = key_item_stock(item_id)
        buyers_key = key_item_buyers(item_id)
        await self._compensate_script(keys=[stock_key, buyers_key], args=[user_id])

        # Publish compensation update to SSE
        remaining = await self.redis_client.get(stock_key)
        channel = key_item_updates_channel(item_id)
        await self.redis_client.publish(
            channel,
            json.dumps({"item_id": item_id, "stock": int(remaining or 0)})
        )
        logger.info(f"[Compensated] Restored 1 stock unit for item {item_id}.")

compensator = WorkerCompensator()
