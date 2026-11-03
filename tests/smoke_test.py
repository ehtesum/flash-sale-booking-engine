import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import time
from uuid import uuid4
import httpx
from shared.config import settings
from shared.schemas import UserClaims
from api.auth import create_jwt_token

API_URL = f"http://127.0.0.1:{settings.API_PORT}/api/v1"

async def main():
    print("=" * 60)
    print("   FLASH-SALE ENGINE: FULL-STACK SMOKE TEST")
    print("=" * 60)

    async with httpx.AsyncClient(timeout=10.0) as client:
        # 1. Health Probe
        print("[1/6] Checking /healthz liveness probe...")
        res = await client.get(f"{API_URL}/healthz")
        assert res.status_code == 200, f"Healthz failed: {res.text}"
        print("      PASS: API Gateway is healthy.")

        # 2. Authenticate as a fresh unique customer
        unique_user_id = uuid4()
        claims = UserClaims(
            user_id=unique_user_id,
            email=f"test_{unique_user_id}@example.com",
            name="Smoke Test Shopper",
            role="customer",
            tier="REGULAR",
        )
        shopper_token = create_jwt_token(claims)
        print(f"[2/6] Generating stateless JWT for user {str(unique_user_id)[:8]}...")
        print("      PASS: Received valid JWT token.")

        # 3. Fetch Catalog
        print("[3/6] Fetching active promotional catalog...")
        res = await client.get(f"{API_URL}/items")
        assert res.status_code == 200, f"Catalog failed: {res.text}"
        items = res.json()
        assert len(items) > 0, "No catalog items found."
        target_item = items[0]
        print(f"      PASS: Found item '{target_item['title'][:35]}...' (Stock: {target_item['available_stock']})")

        # 4. Reserve Item
        print("[4/6] Reserving 1 unit...")
        res = await client.post(
            f"{API_URL}/orders/reserve",
            json={"item_id": target_item["id"]},
            headers={
                "Authorization": f"Bearer {shopper_token}",
                "X-Idempotency-Key": f"idem_smoke_{uuid4()}"
            }
        )
        assert res.status_code == 202, f"Reservation failed: {res.status_code} - {res.text}"
        order_data = res.json()
        print(f"      PASS: Stock secured! Order ID: {order_data['order_id']}")

        # 5. Anti-Double-Dipping Verification
        print("[5/6] Attempting duplicate reservation for same user (anti-scalping test)...")
        dup_res = await client.post(
            f"{API_URL}/orders/reserve",
            json={"item_id": target_item["id"]},
            headers={"Authorization": f"Bearer {shopper_token}"}
        )
        assert dup_res.status_code == 409, f"Expected 409 Conflict, got: {dup_res.status_code}"
        print("      PASS: Successfully blocked duplicate reservation with HTTP 409 Conflict.")

        # 6. Verify Asynchronous Persistence in Database
        print(f"[6/6] Polling order status for {order_data['order_number']}...")
        confirmed = False
        for attempt in range(10):
            await asyncio.sleep(0.5)
            status_res = await client.get(f"{API_URL}/orders/{order_data['order_id']}")
            if status_res.status_code == 200 and status_res.json().get("status") == "CONFIRMED":
                confirmed = True
                break

        assert confirmed, "Order was not confirmed in database within timeout."
        print(f"      PASS: Order confirmed and committed into database!")

    print("=" * 60)
    print("   [SUCCESS] ALL FULL-STACK SMOKE TESTS PASSED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
