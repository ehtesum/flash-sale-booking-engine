import logging
from typing import Protocol, runtime_checkable
from shared.config import settings
from shared.schemas import OrderReservedEvent

logger = logging.getLogger(__name__)

@runtime_checkable
class BrokerProducer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def publish_order_reserved(self, event: OrderReservedEvent) -> None: ...
    async def ping(self) -> bool: ...

def get_event_broker() -> BrokerProducer:
    """DRY factory returning the configured message broker (RabbitMQ or Kafka)."""
    broker_type = settings.MESSAGE_BROKER.lower()
    if broker_type == "rabbitmq":
        from api.rabbitmq_service import rabbitmq_service
        return rabbitmq_service
    elif broker_type == "kafka":
        from api.kafka_service import kafka_service
        return kafka_service
    else:
        logger.warning(f"Unknown MESSAGE_BROKER '{broker_type}'. Defaulting to RabbitMQ.")
        from api.rabbitmq_service import rabbitmq_service
        return rabbitmq_service

event_broker = get_event_broker()

