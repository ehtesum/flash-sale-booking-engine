import asyncio
import json
import logging
from uuid import UUID
import aio_pika
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
from worker.db import persist_order_transaction
from worker.compensator import compensator

logger = logging.getLogger(__name__)

class RabbitMQOrderWorker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.is_running = False

    async def start(self):
        self.is_running = True
        await compensator.start()

        logger.info(f"Connecting worker to RabbitMQ: {settings.RABBITMQ_URL}")
        while self.is_running:
            try:
                self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL, timeout=3.0)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=settings.RABBITMQ_PREFETCH_COUNT)

                # Declare DLX and DLQ
                dlx_exchange = await self.channel.declare_exchange(
                    RABBITMQ_EXCHANGE_DLX,
                    aio_pika.ExchangeType.DIRECT,
                    durable=True,
                )
                dlq = await self.channel.declare_queue(
                    RABBITMQ_QUEUE_DLQ,
                    durable=True,
                )
                await dlq.bind(dlx_exchange, routing_key=RABBITMQ_ROUTING_KEY_DLQ)

                # Declare Main Events Exchange
                events_exchange = await self.channel.declare_exchange(
                    RABBITMQ_EXCHANGE_EVENTS,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )

                # Declare Retry Queue with TTL dead-lettered back to main exchange
                retry_queue = await self.channel.declare_queue(
                    RABBITMQ_QUEUE_RETRY,
                    durable=True,
                    arguments={
                        "x-message-ttl": settings.RABBITMQ_RETRY_DELAY_MS,
                        "x-dead-letter-exchange": RABBITMQ_EXCHANGE_EVENTS,
                        "x-dead-letter-routing-key": RABBITMQ_ROUTING_KEY_RESERVED,
                    },
                )

                # Declare Main Queue with DLX
                self.queue = await self.channel.declare_queue(
                    RABBITMQ_QUEUE_RESERVED,
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": RABBITMQ_EXCHANGE_DLX,
                        "x-dead-letter-routing-key": RABBITMQ_ROUTING_KEY_DLQ,
                    },
                )
                await self.queue.bind(events_exchange, routing_key=RABBITMQ_ROUTING_KEY_RESERVED)

                logger.info(f"[RabbitMQ Worker] Listening on queue '{RABBITMQ_QUEUE_RESERVED}'...")
                break
            except Exception as e:
                logger.warning(f"Waiting for RabbitMQ broker... ({e})")
                await asyncio.sleep(2)

        try:
            await self._consume_loop()
        finally:
            await self.stop()

    async def _consume_loop(self):
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                if not self.is_running:
                    break
                await self._process_message(message)

    async def _process_message(self, message: aio_pika.IncomingMessage):
        payload = {}
        headers = dict(message.headers or {})
        retry_count = int(headers.get("x-retry-count", 0))

        try:
            payload = json.loads(message.body.decode("utf-8"))
            order_id = UUID(str(payload["order_id"]))
            item_id = UUID(str(payload["item_id"]))
            user_id = UUID(str(payload["user_id"]))
            order_number = payload["order_number"]
            amount_cents = payload.get("amount_cents", 0)
            idempotency_key = payload.get("idempotency_key")

            # ACID Transactional Database Persistence
            success = await persist_order_transaction(
                order_id=order_id,
                order_number=order_number,
                item_id=item_id,
                user_id=user_id,
                amount_cents=amount_cents,
                idempotency_key=idempotency_key,
            )

            if success:
                await message.ack()
                logger.info(f"[Worker] Order {order_number} successfully committed to database.")
            else:
                raise RuntimeError("Database write transaction failed")

        except Exception as e:
            logger.error(f"[Worker] Failed to process message (retry {retry_count}/{settings.RABBITMQ_MAX_RETRIES}): {e}")
            if retry_count < settings.RABBITMQ_MAX_RETRIES:
                # Exponential backoff retry via Retry Queue
                headers["x-retry-count"] = retry_count + 1
                try:
                    retry_exchange = self.channel.default_exchange
                    await retry_exchange.publish(
                        aio_pika.Message(
                            body=message.body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            headers=headers,
                        ),
                        routing_key=RABBITMQ_QUEUE_RETRY,
                    )
                    await message.ack()
                except Exception as pub_err:
                    logger.error(f"Failed to publish to retry queue: {pub_err}")
                    await message.nack(requeue=False)  # Dead-letter to DLQ
            else:
                # Exceeded retries: Dead-letter and compensate inventory
                logger.critical(f"[Worker] Message exceeded max retries. Routing to DLQ and compensating stock.")
                await message.nack(requeue=False)
                item_id_str = headers.get("x-item-id") or str(payload.get("item_id", ""))
                user_id_str = headers.get("x-user-id") or str(payload.get("user_id", ""))
                if item_id_str and user_id_str:
                    await compensator.restore_stock(item_id_str, user_id_str)

    async def stop(self):
        self.is_running = False
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        await compensator.stop()
        logger.info("RabbitMQ worker shut down cleanly.")

