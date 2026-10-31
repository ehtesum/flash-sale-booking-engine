import asyncio
import json
import logging
import signal
from uuid import UUID
from aiokafka import AIOKafkaConsumer
from shared.config import settings
from shared.constants import TOPIC_ORDERS_RESERVED
from worker.db import persist_order_transaction
from worker.compensator import compensator

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "service": "flashsale-worker", "message": "%(message)s"}'
)
logger = logging.getLogger(__name__)

class FlashSaleOrderWorker:
    def __init__(self):
        self.consumer: AIOKafkaConsumer | None = None
        self.is_running = False

    async def start(self):
        self.is_running = True
        await compensator.start()

        logger.info(f"Connecting worker to Kafka: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        self.consumer = AIOKafkaConsumer(
            TOPIC_ORDERS_RESERVED,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        while self.is_running:
            try:
                await self.consumer.start()
                logger.info(f"Worker listening on topic '{TOPIC_ORDERS_RESERVED}'...")
                break
            except Exception as e:
                logger.warning(f"Waiting for Kafka brokers... ({e})")
                await asyncio.sleep(2)

        try:
            await self._consume_loop()
        finally:
            await self.stop()

    async def _consume_loop(self):
        while self.is_running:
            try:
                msg_batch = await self.consumer.getmany(timeout_ms=1000, max_records=200)
                if not msg_batch:
                    continue

                for tp, messages in msg_batch.items():
                    for msg in messages:
                        await self._process_message(msg.value)
                    
                    await self.consumer.commit({tp: messages[-1].offset + 1})

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _process_message(self, data: dict):
        order_number = data.get("order_number", "UNKNOWN")

        try:
            order_id = UUID(data["order_id"])
            item_id = UUID(data["item_id"])
            user_id = UUID(data["user_id"])
            amount_cents = int(data.get("amount_cents", 0))
            idempotency_key = data.get("idempotency_key")

            await persist_order_transaction(
                order_id=order_id,
                order_number=order_number,
                item_id=item_id,
                user_id=user_id,
                amount_cents=amount_cents,
                idempotency_key=idempotency_key,
            )

        except Exception as err:
            logger.error(f"Persistence failed for order {order_number}: {err}")
            await compensator.handle_failure(
                event_data=data,
                reason="DatabaseTransactionFailed",
                error_details={"error": str(err)},
            )

    async def stop(self):
        self.is_running = False
        if self.consumer:
            await self.consumer.stop()
        await compensator.close()
        logger.info("Worker stopped.")

async def main():
    worker = FlashSaleOrderWorker()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
        except NotImplementedError:
            pass

    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
