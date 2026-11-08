import asyncio
from httpx import AsyncClient, ASGITransport
from api.main import app
from api.db import init_db
from api.redis_service import redis_service

async def run_smoke():
    print("=" * 60)
    print("   END-TO-END SMOKE VERIFICATION TEST")
    print("=" * 60)
    await init_db()
    await redis_service.connect()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Trigger FastAPI lifespan to start background worker and connections
        async with app.router.lifespan_context(app):
            # 1. Health Probe
            res = await client.get("/api/v1/healthz")
            assert res.status_code == 200, f"Healthz failed: {res.text}"
            print("[1/6] PASS: Health probe is operational.")

            # 2. Fresh Unique Shopper Authentication
            from uuid import uuid4
            from shared.schemas import UserClaims
            from api.auth import create_jwt_token

            test_user_id = uuid4()
            claims = UserClaims(
                user_id=test_user_id,
                email=f"e2e_{test_user_id}@example.com",
                name="E2E Shopper",
                role="customer",
                tier="REGULAR",
            )
            token = create_jwt_token(claims)
            print("[2/6] PASS: JWT auth token generated.")

            # 3. Catalog & Cache-Aside
            res_cat = await client.get("/api/v1/items")
            item_id = res_cat.json()[0]["id"]
            await redis_service.set_stock(str(item_id), 100)
            await redis_service.invalidate_catalog_cache()

            res = await client.get("/api/v1/items")
            assert res.status_code == 200, f"Catalog failed: {res.text}"
            items = res.json()
            assert len(items) > 0, "No catalog items found"
            cached = await redis_service.get_cached_catalog()
            assert cached is not None, "Catalog was not cached in Redis"
            print(f"[3/6] PASS: Catalog returned and verified cached in Redis (Item: {items[0]['title'][:30]}).")

            # 4. Atomic In-Memory Reservation
            res = await client.post(
                "/api/v1/orders/reserve",
                json={"item_id": item_id},
                headers={"Authorization": f"Bearer {token}"}
            )
            assert res.status_code == 202, f"Reserve failed: {res.text}"
            order = res.json()
            print(f"[4/6] PASS: Inventory reserved atomically. Order #{order['order_number']}.")

            # 5. Anti-Double-Dipping 409
            dup_res = await client.post(
                "/api/v1/orders/reserve",
                json={"item_id": item_id},
                headers={"Authorization": f"Bearer {token}"}
            )
            assert dup_res.status_code == 409, f"Expected 409, got {dup_res.status_code}"
            print("[5/6] PASS: Duplicate reservation attempt blocked with HTTP 409 Conflict.")

            # 6. Background Worker Persistence
            await asyncio.sleep(0.5)
            order_res = await client.get(f"/api/v1/orders/{order['order_id']}")
            assert order_res.status_code == 200, f"Order status failed: {order_res.text}"
            assert order_res.json()["status"] == "CONFIRMED"
            print("[6/6] PASS: Order confirmed and committed to database.")

    print("=" * 60)
    print("   ALL 6 END-TO-END VERIFICATION CHECKS PASSED")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_smoke())
