import logging
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from shared.database import engine, AsyncSessionLocal, is_sqlite, check_db_health

logger = logging.getLogger(__name__)

INITIAL_CATALOG = [
    {
        "id": "a0000000-0000-0000-0000-000000000001",
        "sku": "AGODA-MALDIVES-VILLA-DROP",
        "title": "Agoda Exclusive: Overwater Villa Maldives (3 Nights) — 90% Flash Drop",
        "category": "TRAVEL_HOTEL",
        "total_stock": 10,
        "price_cents": 14900,
    },
    {
        "id": "b0000000-0000-0000-0000-000000000002",
        "sku": "SHOPEE-1111-IPHONE16-MAX",
        "title": "Shopee 11.11 Mega Drop: iPhone 16 Pro Max 256GB Desert Titanium",
        "category": "ELECTRONICS",
        "total_stock": 50,
        "price_cents": 89900,
    },
    {
        "id": "c0000000-0000-0000-0000-000000000003",
        "sku": "CAREEM-DUBAI-SUPERCAR-PASS",
        "title": "Careem Dubai SuperCar Chauffeur VIP Pass (McLaren / Ferrari Transfer)",
        "category": "MOBILITY_SERVICES",
        "total_stock": 25,
        "price_cents": 4900,
    },
    {
        "id": "d0000000-0000-0000-0000-000000000004",
        "sku": "CONCURRENCY-STRESS-BENCHMARK-ITEM",
        "title": "High-Concurrency Flash-Sale Benchmark Item (50 Units)",
        "category": "BENCHMARK",
        "total_stock": 50,
        "price_cents": 1000,
    },
]

async def init_db():
    """Initializes schema if running SQLite in standalone development mode."""
    if is_sqlite:
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS items (
                    id TEXT PRIMARY KEY,
                    sku TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    category TEXT NOT NULL,
                    total_stock INTEGER NOT NULL,
                    reserved_stock INTEGER NOT NULL DEFAULT 0,
                    price_cents INTEGER NOT NULL,
                    sale_start TEXT NOT NULL,
                    sale_end TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """))
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    order_number TEXT UNIQUE NOT NULL,
                    user_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'RESERVED',
                    idempotency_key TEXT UNIQUE,
                    amount_cents INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(user_id, item_id)
                );
            """))
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS inventory_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT NOT NULL,
                    order_id TEXT,
                    delta INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT NOT NULL
                );
            """))

            count_res = await conn.execute(text("SELECT COUNT(*) FROM items"))
            if count_res.scalar() == 0:
                now_str = datetime.now(timezone.utc).isoformat()
                end_str = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
                for itm in INITIAL_CATALOG:
                    await conn.execute(
                        text("""
                            INSERT INTO items (id, sku, title, category, total_stock, reserved_stock, price_cents, sale_start, sale_end, created_at, updated_at)
                            VALUES (:id, :sku, :title, :category, :total_stock, 0, :price_cents, :start, :end, :now, :now)
                        """),
                        {**itm, "start": now_str, "end": end_str, "now": now_str}
                    )
                logger.info("[Database] Initialized items in SQLite catalog.")

async def get_item_by_id(item_id: UUID | str) -> Optional[Dict[str, Any]]:
    query = text("""
        SELECT id, sku, title, category, total_stock, reserved_stock, 
               (total_stock - reserved_stock) as available_stock,
               price_cents, sale_start, sale_end
        FROM items
        WHERE id = :item_id
    """)
    async with AsyncSessionLocal() as session:
        result = await session.execute(query, {"item_id": str(item_id)})
        row = result.mappings().first()
        return dict(row) if row else None

async def list_active_items() -> List[Dict[str, Any]]:
    query = text("""
        SELECT id, sku, title, category, total_stock, reserved_stock, 
               (total_stock - reserved_stock) as available_stock,
               price_cents, sale_start, sale_end
        FROM items
        ORDER BY created_at ASC
    """)
    async with AsyncSessionLocal() as session:
        result = await session.execute(query)
        return [dict(row) for row in result.mappings().all()]

async def get_order_by_id(order_id: UUID | str) -> Optional[Dict[str, Any]]:
    query = text("""
        SELECT id, order_number, item_id, user_id, status, amount_cents, created_at, updated_at
        FROM orders
        WHERE id = :order_id
    """)
    async with AsyncSessionLocal() as session:
        result = await session.execute(query, {"order_id": str(order_id)})
        row = result.mappings().first()
        return dict(row) if row else None
