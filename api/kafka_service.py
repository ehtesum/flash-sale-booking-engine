import asyncio
import json
import logging
from typing import Optional
from shared.config import settings
from shared.constants import TOPIC_ORDERS_RESERVED
from shared.schemas import OrderReservedEvent

logger = logging.getLogger(__name__)

class KafkaProducerService:
    def __init__(self):
        self.producer = None
        self.in_memory_queue: asyncio.Queue = asyncio.Queue()
        self.is_standalone = False

    async def start(self):
        if settings.STANDALONE_MODE:
            logger.info("[Kafka] Operating in Standalone Zero-Docker mode (in-memory async streaming queue).")
            self.is_standalone = True
            return

        try:
            from aiokafka import AIOKafkaProducer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",
                retry_backoff_ms=100,
            )
            await self.producer.start()
            logger.info("[Kafka] Successfully connected to Kafka cluster.")
        except Exception as e:
            logger.warning(f"[Kafka] Kafka broker unreachable ({e}). Falling back to in-memory async streaming queue.")
            self.is_standalone = True

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def publish_order_reserved(self, event: OrderReservedEvent):
        """Asynchronously writes the reservation event to Kafka or the standalone buffer."""
        if self.is_standalone:
            await self.in_memory_queue.put(event.model_dump(mode="json"))
            return

        if not self.producer:
            raise RuntimeError("Kafka producer is not running")

        await self.producer.send_and_wait(
            topic=TOPIC_ORDERS_RESERVED,
            key=str(event.item_id),
            value=event.model_dump(mode="json"),
            headers=[("correlation_id", event.correlation_id.encode("utf-8"))]
        )

    async def ping(self) -> bool:
        if self.is_standalone:
            return True
        if not self.producer:
            return False
        try:
            return bool(self.producer.client.cluster.brokers())
        except Exception:
            return False

kafka_service = KafkaProducerService()
