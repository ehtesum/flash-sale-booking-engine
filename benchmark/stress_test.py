import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import time
from uuid import uuid4
import httpx
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from shared.config import settings
from shared.schemas import UserClaims
from api.auth import create_jwt_token

BENCHMARK_ITEM_ID = "d0000000-0000-0000-0000-000000000004"
TOTAL_REQUESTS = 1000
EXPECTED_STOCK = 50
API_BASE_URL = f"http://127.0.0.1:{settings.API_PORT}/api/v1"

async def run_single_request(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, user_index: int, results: list):
    user_id = uuid4()
    claims = UserClaims(
        user_id=user_id,
        email=f"shopper_{user_index}@benchmark.test",
        name=f"Shopper {user_index}",
        role="customer",
        tier="REGULAR",
    )
    token = create_jwt_token(claims)
    
    async with semaphore:
        start_time = time.perf_counter()
        try:
            response = await client.post(
                f"{API_BASE_URL}/orders/reserve",
                json={"item_id": BENCHMARK_ITEM_ID},
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Correlation-ID": f"bench_{user_index}",
                    "X-Idempotency-Key": f"idem_bench_{user_id}",
                },
                timeout=30.0,
            )
            latency_ms = (time.perf_counter() - start_time) * 1000
            results.append((response.status_code, latency_ms))
        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000
            results.append((500, latency_ms))

async def reset_benchmark_stock():
    """Calls server admin endpoint to reset Redis stock and DB orders."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        res = await client.post(
            f"{API_BASE_URL}/admin/reset-stock",
            params={"item_id": BENCHMARK_ITEM_ID, "stock": EXPECTED_STOCK}
        )
        assert res.status_code == 200, f"Reset failed: {res.text}"

async def verify_database_integrity():
    is_sqlite = "sqlite" in settings.DATABASE_URL
    kwargs = {"connect_args": {"check_same_thread": False}} if is_sqlite else {}
    engine = create_async_engine(settings.DATABASE_URL, echo=False, **kwargs)
    
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT 
                    items.total_stock,
                    COUNT(orders.id) as confirmed_orders
                FROM items
                LEFT JOIN orders ON items.id = orders.item_id AND orders.status = 'CONFIRMED'
                WHERE items.id = :id
                GROUP BY items.total_stock;
            """),
            {"id": BENCHMARK_ITEM_ID}
        )
        row = result.mappings().first()
        await engine.dispose()
        return dict(row) if row else {"total_stock": EXPECTED_STOCK, "confirmed_orders": 0}

async def main():
    print("=" * 65)
    print("   DISTRIBUTED FLASH-SALE ENGINE: 1,000-USER STRESS BENCHMARK")
    print("=" * 65)
    print("Resetting benchmark item stock to 50 on live server...")
    await reset_benchmark_stock()

    print(f"Target Item ID:     {BENCHMARK_ITEM_ID}")
    print(f"Initial Stock:      {EXPECTED_STOCK} units")
    print(f"Simulated Shoppers: {TOTAL_REQUESTS} concurrent virtual users")
    print("-" * 65)
    print("Dispatching 1,000 concurrent asynchronous requests...")

    results = []
    semaphore = asyncio.Semaphore(100)  # 100 concurrent workers
    wall_start = time.perf_counter()

    limits = httpx.Limits(max_connections=150, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            run_single_request(client, semaphore, i, results)
            for i in range(TOTAL_REQUESTS)
        ]
        await asyncio.gather(*tasks)

    total_wall_time = time.perf_counter() - wall_start
    print(f"All {TOTAL_REQUESTS} requests completed in {total_wall_time:.2f} seconds.")

    status_counts = {}
    latencies = []
    for status_code, lat in results:
        status_counts[status_code] = status_counts.get(status_code, 0) + 1
        latencies.append(lat)

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    successful_202 = status_counts.get(202, 0)
    sold_out_410 = status_counts.get(410, 0)
    conflict_409 = status_counts.get(409, 0)
    errors_5xx = sum(count for code, count in status_counts.items() if code >= 500)

    print("-" * 65)
    print("HTTP RESPONSE BREAKDOWN:")
    print(f"  * 202 Accepted (Stock Secured):  {successful_202:5d}  (Expected: {EXPECTED_STOCK})")
    print(f"  * 410 Gone     (Sold Out):       {sold_out_410:5d}  (Expected: {TOTAL_REQUESTS - EXPECTED_STOCK})")
    print(f"  * 409 Conflict (Duplicates):     {conflict_409:5d}")
    print(f"  * 5xx Errors   (Crashes):        {errors_5xx:5d}")
    print("-" * 65)
    print("LATENCY PERCENTILES:")
    print(f"  * p50: {p50:6.2f} ms")
    print(f"  * p95: {p95:6.2f} ms")
    print(f"  * p99: {p99:6.2f} ms")
    print("-" * 65)

    print("Waiting 2 seconds for worker to drain buffer into database...")
    await asyncio.sleep(2.0)

    db_stats = await verify_database_integrity()
    confirmed_in_db = db_stats["confirmed_orders"]
    oversold_count = max(0, confirmed_in_db - EXPECTED_STOCK)

    print("DATABASE PERSISTENCE VERIFICATION:")
    print(f"  * Initial Catalog Stock:         {db_stats['total_stock']}")
    print(f"  * Confirmed Orders in DB:        {confirmed_in_db}")
    print(f"  * OVERSOLD COUNT:                {oversold_count}")
    print("=" * 65)

    if successful_202 == EXPECTED_STOCK and oversold_count == 0 and errors_5xx == 0:
        print("  [PASS] Concurrency verification: 50 orders committed, 0 oversold, 0 server errors.")
    else:
        print("  [FAIL] Verification failed: data consistency or error threshold breached.")
    print("=" * 65)

if __name__ == "__main__":
    asyncio.run(main())
