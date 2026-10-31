import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from shared.config import settings

logger = logging.getLogger(__name__)

is_sqlite = "sqlite" in settings.DATABASE_URL
engine_kwargs = {}
if is_sqlite:
    engine_kwargs = {"connect_args": {"check_same_thread": False}}
else:
    engine_kwargs = {
        "pool_size": 20,
        "max_overflow": 10,
        "pool_timeout": 5,
        "pool_recycle": 1800,
    }

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    **engine_kwargs
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def check_db_health() -> bool:
    """Verifies relational database connectivity."""
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception as e:
        logger.warning(f"Database health check failed: {e}")
        return False

