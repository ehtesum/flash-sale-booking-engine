import os
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from api.routes import router
from api.redis_service import redis_service
from api.kafka_service import kafka_service
from api.db import init_db, list_active_items
from worker.db import persist_order_transaction
from shared.config import settings

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "service": "flashsale-api", "message": "%(message)s"}'
)
logger = logging.getLogger(__name__)

async def _standalone_worker_loop():
    """Background consumer worker for development mode."""
    logger.info("Standalone in-memory persistence worker started.")
    while True:
        try:
            event = await kafka_service.in_memory_queue.get()
            order_number = event.get("order_number")

            await persist_order_transaction(
                order_id=event["order_id"],
                order_number=order_number,
                item_id=event["item_id"],
                user_id=event["user_id"],
                amount_cents=event.get("amount_cents", 0),
                idempotency_key=event.get("idempotency_key"),
            )
            kafka_service.in_memory_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error persisting order: {e}", exc_info=True)
            await asyncio.sleep(0.5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing services...")
    await init_db()
    await redis_service.connect()
    await kafka_service.start()

    # Pre-warm Redis inventory
    items = await list_active_items()
    for itm in items:
        existing = await redis_service.client.get(f"item:{itm['id']}:stock")
        if existing is None:
            await redis_service.set_stock(str(itm["id"]), itm["total_stock"])

    worker_task = None
    if kafka_service.is_standalone:
        worker_task = asyncio.create_task(_standalone_worker_loop())

    logger.info(f"Flash-Sale Engine ready on port {settings.API_PORT}")
    yield

    if worker_task:
        worker_task.cancel()
    await kafka_service.stop()
    await redis_service.close()
    logger.info("Services stopped cleanly.")

app = FastAPI(
    title="Flash-Sale & Reservation Engine",
    description="High-concurrency reservation engine using Redis atomic Lua, Kafka streaming, and PostgreSQL/SQLite persistence.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_dashboard():
        return FileResponse(os.path.join(static_dir, "index.html"))
