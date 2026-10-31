import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import datetime, timezone
from sqlalchemy import text
from shared.database import engine, AsyncSessionLocal
from shared.constants import key_item_stock, key_item_buyers
from api.redis_service import redis_service

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

CATALOG_ITEMS = [
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

async def seed_database():
    now_str = datetime.now(timezone.utc).isoformat()
    async with engine.begin() as conn:
        logger.info("[Database] Refreshing catalog items...")
        for itm in CATALOG_ITEMS:
            await conn.execute(
                text("""
                    INSERT INTO items (id, sku, title, category, total_stock, reserved_stock, price_cents, sale_start, sale_end, created_at, updated_at)
                    VALUES (:id, :sku, :title, :category, :total_stock, 0, :price_cents, :now, :now, :now, :now)
                    ON CONFLICT (sku) DO UPDATE 
                    SET total_stock = EXCLUDED.total_stock,
                        reserved_stock = 0,
                        updated_at = :now;
                """),
                {**itm, "now": now_str}
            )
            if itm["category"] == "BENCHMARK":
                await conn.execute(
                    text("DELETE FROM orders WHERE item_id = :id"),
                    {"id": itm["id"]}
                )
    logger.info("[Database] Catalog seeded.")

async def prewarm_redis():
    logger.info("[Redis] Pre-warming inventory keys...")
    await redis_service.connect()
    
    for itm in CATALOG_ITEMS:
        item_id = itm["id"]
        stock_key = key_item_stock(item_id)
        buyers_key = key_item_buyers(item_id)
        
        await redis_service.client.set(stock_key, itm["total_stock"])
        await redis_service.client.delete(buyers_key)
        logger.info(f"  * {itm['sku']}: Pre-warmed stock = {itm['total_stock']} units")

    await redis_service.close()
    logger.info("[Redis] Inventory keys pre-warmed.")

async def main():
    print("Seeding catalog...")
    await seed_database()
    await prewarm_redis()
    print("Catalog and Redis ready.")

if __name__ == "__main__":
    asyncio.run(main())
