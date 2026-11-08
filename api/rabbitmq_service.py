import asyncio
import json
import logging
from typing import Optional
from shared.config import settings
from shared.constants import (
    RABBITMQ_EXCHANGE_EVENTS,
    RABBITMQ_ROUTING_KEY_RESERVED,
    RABBITMQ_EXCHANGE_DLX,
    RABBITMQ_QUEUE_RESERVED,
    RABBITMQ_QUEUE_DLQ,
    RABBITMQ_ROUTING_KEY_DLQ,
    RABBITMQ_QUEUE_RETRY,
)
from shared.schemas import OrderReservedEvent

logger = logging.getLogger(__name__)

class RabbitMQProducerService:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None
        self.in_memory_queue: asyncio.Queue = asyncio.Queue()
        self.is_standalone = False

    async def start(self):
        if settings.STANDALONE_MODE:
            logger.info("[RabbitMQ] Operating in Standalone Zero-Docker mode (in-memory async queue fallback).")
            self.is_standalone = True
            return

        try:
            import aio_pika
            self.connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                timeout=3.0,
            )
            # Create channel with publisher confirms enabled for reliability
            self.channel = await self.connection.channel(publisher_confirms=True)

            # Declare Dead-Letter Exchange (fanout/direct)
            dlx_exchange = await self.channel.declare_exchange(
                RABBITMQ_EXCHANGE_DLX,
                aio_pika.ExchangeType.DIRECT,
                durable=True,
            )

            # Declare DLQ queue
            dlq = await self.channel.declare_queue(
                RABBITMQ_QUEUE_DLQ,
                durable=True,
            )
            await dlq.bind(dlx_exchange, routing_key=RABBITMQ_ROUTING_KEY_DLQ)

            # Declare Main Events Topic Exchange
            self.exchange = await self.channel.declare_exchange(
                RABBITMQ_EXCHANGE_EVENTS,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )

            # Declare Main Queue with DLX configuration
            queue = await self.channel.declare_queue(
                RABBITMQ_QUEUE_RESERVED,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": RABBITMQ_EXCHANGE_DLX,
                    "x-dead-letter-routing-key": RABBITMQ_ROUTING_KEY_DLQ,
                },
            )
            await queue.bind(self.exchange, routing_key=RABBITMQ_ROUTING_KEY_RESERVED)

            logger.info("[RabbitMQ] Connected & declared durable exchanges and DLQ topology.")
        except Exception as e:
            logger.warning(f"[RabbitMQ] Broker unreachable ({e}). Falling back to in-memory async queue.")
            self.is_standalone = True

    async def stop(self):
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

    async def publish_order_reserved(self, event: OrderReservedEvent):
        """Publishes reservation event to RabbitMQ topic exchange with persistent delivery mode."""
        if self.is_standalone or not self.exchange or (self.channel and self.channel.is_closed):
            # Graceful fallback to standalone queue if broker is unreachable
            await self.in_memory_queue.put(event.model_dump(mode="json"))
            return

        import aio_pika
        body = json.dumps(event.model_dump(mode="json"), default=str).encode("utf-8")
        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            correlation_id=event.correlation_id,
            message_id=str(event.order_id),
            headers={
                "x-retry-count": 0,
                "x-item-id": str(event.item_id),
                "x-user-id": str(event.user_id),
            },
        )
        # Confirmed publish
        await self.exchange.publish(
            message,
            routing_key=RABBITMQ_ROUTING_KEY_RESERVED,
        )

    async def ping(self) -> bool:
        if self.is_standalone:
            return True
        return bool(self.connection and not self.connection.is_closed)

rabbitmq_service = RabbitMQProducerService()

