import asyncio
from uuid import uuid4
import pytest
from httpx import AsyncClient, ASGITransport
from api.main import app
from shared.schemas import UserClaims, ReservationRequest
from api.auth import create_jwt_token
from api.redis_service import redis_service
from api.db import init_db

@pytest.fixture(autouse=True)
def setup_environment(monkeypatch):
    monkeypatch.setenv("STANDALONE_MODE", "true")

@pytest.fixture
def auth_headers():
    user_id = uuid4()
    claims = UserClaims(
        user_id=user_id,
        email=f"test_{user_id}@example.com",
        name="Unit Test Shopper",
        role="customer",
        tier="VIP",
    )
    token = create_jwt_token(claims)
    return {"Authorization": f"Bearer {token}"}, user_id

@pytest.mark.asyncio
async def test_health_check():
    await init_db()
    await redis_service.connect()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        res = await ac.get("/api/v1/healthz")
        assert res.status_code == 200
        assert res.json()["status"].lower() == "healthy"

@pytest.mark.asyncio
async def test_catalog_caching():
    await init_db()
    await redis_service.connect()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # First call loads and caches
        res1 = await ac.get("/api/v1/items")
        assert res1.status_code == 200
        items1 = res1.json()
        assert len(items1) > 0

        # Check redis cache exists
        cached = await redis_service.get_cached_catalog()
        assert cached is not None
        assert len(cached) == len(items1)

        # Second call should serve successfully with fresh live stock
        res2 = await ac.get("/api/v1/items")
        assert res2.status_code == 200
        assert res2.json() == items1

@pytest.mark.asyncio
async def test_rate_limiting_exceeded(auth_headers):
    await init_db()
    await redis_service.connect()
    headers, user_id = auth_headers
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Get an active item
        cat_res = await ac.get("/api/v1/items")
        item_id = cat_res.json()[0]["id"]

        # Rapidly fire requests exceeding limit (5 allowed in 10s window)
        statuses = []
        for i in range(7):
            req_body = {"item_id": item_id}
            res = await ac.post("/api/v1/orders/reserve", json=req_body, headers=headers)
            statuses.append(res.status_code)

        # Should encounter 429 Too Many Requests
        assert 429 in statuses, f"Expected 429 in response statuses: {statuses}"

@pytest.mark.asyncio
async def test_rabbitmq_and_broker_factory():
    from api.broker_factory import get_event_broker, BrokerProducer
    broker = get_event_broker()
    assert isinstance(broker, BrokerProducer)
    assert hasattr(broker, "publish_order_reserved")
