import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application & Mode
    ENVIRONMENT: str = "development"
    STANDALONE_MODE: bool = True  # True for Zero-Docker mode (in-memory Redis & SQLite)
    LOG_LEVEL: str = "INFO"
    API_PORT: int = 8000

    # JWT Authentication
    JWT_SECRET: str = "flashsale-super-secret-key-change-in-prod-2026"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 60

    # Redis In-Memory Configuration
    REDIS_URL: str = "redis://localhost:6379/0"

    # Message Broker Configuration ("kafka" or "rabbitmq")
    MESSAGE_BROKER: str = "rabbitmq"

    # Kafka Broker Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_RESERVED: str = "orders.reserved"
    KAFKA_TOPIC_DLQ: str = "orders.dlq"
    KAFKA_CONSUMER_GROUP: str = "flashsale-order-workers"

    # RabbitMQ Configuration
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_PREFETCH_COUNT: int = 10
    RABBITMQ_MAX_RETRIES: int = 3
    RABBITMQ_RETRY_DELAY_MS: int = 2000

    # Rate Limiting Configuration
    RATE_LIMIT_REQUESTS: int = 5
    RATE_LIMIT_WINDOW_SECONDS: int = 10

    # Database Configuration (SQLite for Zero-Docker mode, PostgreSQL for Docker mode)
    DATABASE_URL: str = "sqlite+aiosqlite:///./flashsale.db"

# Global settings singleton
settings = Settings()
