import os
from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
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

    # Kafka Broker Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_RESERVED: str = "orders.reserved"
    KAFKA_TOPIC_DLQ: str = "orders.dlq"
    KAFKA_CONSUMER_GROUP: str = "flashsale-order-workers"

    # Database Configuration (SQLite for Zero-Docker mode, PostgreSQL for Docker mode)
    DATABASE_URL: str = "sqlite+aiosqlite:///./flashsale.db"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

# Global settings singleton
settings = Settings()
