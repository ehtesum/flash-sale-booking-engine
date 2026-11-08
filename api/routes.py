import asyncio
import json
import logging
from uuid import UUID, uuid4
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Header, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import text
from shared.constants import LuaReturnCode, key_item_updates_channel
from shared.schemas import (
    ReservationRequest,
    ReservationResponse,
    ItemSchema,
    OrderSchema,
    OrderReservedEvent,
    ProblemDetails,
    DemoLoginRequest,
    TokenResponse,
    UserClaims,
)
from shared.database import AsyncSessionLocal, check_db_health
from api.redis_service import redis_service
from api.broker_factory import event_broker
from api.auth import get_current_user, create_jwt_token, DEMO_PERSONAS
from api.db import get_item_by_id, list_active_items, get_order_by_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1")

@router.post("/admin/reset-stock")
async def reset_stock_endpoint(item_id: UUID, stock: int = 50):
    """Admin endpoint to reset inventory and clear orders for clean test runs."""
    item_id_str = str(item_id)
    await redis_service.set_stock(item_id_str, stock)
    await redis_service.invalidate_catalog_cache()
    
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(
                text("DELETE FROM orders WHERE item_id = :id"),
                {"id": item_id_str}
            )
            await session.execute(
                text("UPDATE items SET reserved_stock = 0 WHERE id = :id"),
                {"id": item_id_str}
            )
    return {"status": "RESET_SUCCESSFUL", "item_id": item_id, "stock": stock}

@router.post("/auth/demo-token", response_model=TokenResponse)
async def generate_demo_token(req: DemoLoginRequest):
    persona_key = req.persona.lower()
    if persona_key not in DEMO_PERSONAS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown persona. Valid options: {list(DEMO_PERSONAS.keys())}"
        )

    claims = UserClaims(**DEMO_PERSONAS[persona_key])
    token = create_jwt_token(claims)
    return TokenResponse(
        access_token=token,
        user_id=claims.user_id,
        name=claims.name,
        tier=claims.tier,
    )

@router.get("/items", response_model=list[ItemSchema])
async def get_catalog():
    # Cache-Aside: check Redis cache for product metadata
    cached_catalog = await redis_service.get_cached_catalog()
    if cached_catalog is not None:
        # Enforce fresh real-time stock lookup (< 1ms) to eliminate stale inventory reads
        for itm in cached_catalog:
            itm["available_stock"] = await redis_service.get_stock(str(itm["id"]))
        return cached_catalog

    # Cache miss: read from DB and populate Redis
    items = await list_active_items()
    enriched = []
    for itm in items:
        live_stock = await redis_service.get_stock(str(itm["id"]))
        itm["available_stock"] = live_stock
        enriched.append(itm)

    await redis_service.set_cached_catalog(enriched)
    return enriched

@router.post(
    "/orders/reserve",
    response_model=ReservationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        409: {"model": ProblemDetails, "description": "Duplicate purchase attempt"},
        410: {"model": ProblemDetails, "description": "Flash sale inventory depleted"},
        429: {"model": ProblemDetails, "description": "Too many requests (rate limit exceeded)"},
    }
)
async def reserve_item(
    req: ReservationRequest,
    user: UserClaims = Depends(get_current_user),
    x_correlation_id: str | None = Header(None),
    x_idempotency_key: str | None = Header(None),
):
    correlation_id = x_correlation_id or f"corr_{uuid4()}"
    item_id_str = str(req.item_id)
    user_id_str = str(user.user_id)

    # 0. Redis Sliding-Window Rate Limiting
    is_allowed = await redis_service.check_rate_limit(user_id_str)
    if not is_allowed:
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content=ProblemDetails(
                type="https://api.flashsale.internal/errors/rate-limit-exceeded",
                title="Rate Limit Exceeded",
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Request velocity exceeded. Please slow down.",
                instance="/api/v1/orders/reserve",
                code="RATE_LIMIT_EXCEEDED",
            ).model_dump(mode="json"),
            headers={"Retry-After": "10"}
        )

    # 1. Atomic in-memory inventory decrement (< 2ms)
    result = await redis_service.reserve_stock(item_id_str, user_id_str)

    if result == LuaReturnCode.SOLD_OUT:
        return JSONResponse(
            status_code=status.HTTP_410_GONE,
            content=ProblemDetails(
                type="https://api.flashsale.internal/errors/inventory-depleted",
                title="Inventory Depleted",
                status=status.HTTP_410_GONE,
                detail="All promotional units for this item have been claimed.",
                instance="/api/v1/orders/reserve",
                code="INVENTORY_DEPLETED",
            ).model_dump(mode="json"),
        )

    if result == LuaReturnCode.ALREADY_PURCHASED:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=ProblemDetails(
                type="https://api.flashsale.internal/errors/duplicate-purchase",
                title="Duplicate Purchase Attempt",
                status=status.HTTP_409_CONFLICT,
                detail="Each shopper is limited to 1 purchase per flash-sale drop.",
                instance="/api/v1/orders/reserve",
                code="DUPLICATE_PURCHASE",
            ).model_dump(mode="json"),
        )

    # 2. Fetch actual item price dynamically
    item_record = await get_item_by_id(req.item_id)
    amount_cents = item_record["price_cents"] if item_record else 0

    order_id = uuid4()
    order_number = f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(order_id)[:8].upper()}"
    
    event = OrderReservedEvent(
        order_id=order_id,
        order_number=order_number,
        item_id=req.item_id,
        user_id=user.user_id,
        amount_cents=amount_cents,
        idempotency_key=x_idempotency_key,
        correlation_id=correlation_id,
    )

    # 3. Publish to streaming broker (decoupling client from DB latency)
    try:
        await event_broker.publish_order_reserved(event)
    except Exception as e:
        logger.error(f"Failed to publish reservation event for {order_number}: {e}")
        await redis_service.compensate_stock(item_id_str, user_id_str)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Buffer unavailable. Stock compensated.",
        )

    # 4. Notify live SSE subscribers
    remaining = await redis_service.get_stock(item_id_str)
    await redis_service.publish_stock_update(item_id_str, remaining)

    return ReservationResponse(
        order_id=order_id,
        order_number=order_number,
        item_id=req.item_id,
        user_id=user.user_id,
        status="RESERVED",
        message="Stock secured. Order persisting asynchronously.",
        check_url=f"/api/v1/orders/{order_id}",
    )

@router.get("/orders/{order_id}", response_model=OrderSchema)
async def get_order_status(order_id: UUID):
    order = await get_order_by_id(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found or processing in queue.",
        )
    return order

@router.get("/items/{item_id}/stream")
async def stream_item_stock(item_id: UUID, request: Request):
    """Server-Sent Events endpoint streaming live stock decrements."""
    async def event_generator():
        channel_name = key_item_updates_channel(str(item_id))
        pubsub = redis_service.client.pubsub()
        await pubsub.subscribe(channel_name)
        try:
            initial_stock = await redis_service.get_stock(str(item_id))
            yield f"data: {json.dumps({'item_id': str(item_id), 'stock': initial_stock})}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    yield f"data: {message['data']}\n\n"
                await asyncio.sleep(0.1)
        finally:
            await pubsub.unsubscribe(channel_name)
            await pubsub.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@router.get("/healthz", tags=["Health"])
async def liveness_probe():
    return {"status": "healthy", "service": "flashsale-api"}

@router.get("/ready", tags=["Health"])
async def readiness_probe():
    redis_ok = await redis_service.ping()
    db_ok = await check_db_health()
    kafka_ok = await kafka_service.ping()

    if not (redis_ok and db_ok and kafka_ok):
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "degraded",
                "redis": redis_ok,
                "postgres": db_ok,
                "kafka": kafka_ok,
            }
        )
    return {
        "status": "ready",
        "redis": True,
        "postgres": True,
        "kafka": True,
    }
