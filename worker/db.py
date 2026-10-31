import logging
from uuid import UUID
from datetime import datetime, timezone
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from shared.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

async def persist_order_transaction(
    order_id: UUID | str,
    order_number: str,
    item_id: UUID | str,
    user_id: UUID | str,
    amount_cents: int,
    idempotency_key: str | None,
) -> bool:
    """
    Executes an ACID transaction inserting the confirmed order and updating inventory.
    Enforces idempotency using ON CONFLICT (idempotency_key) DO NOTHING.
    """
    now_str = datetime.now(timezone.utc).isoformat()
    order_id_str = str(order_id)
    item_id_str = str(item_id)
    user_id_str = str(user_id)
    idem_key_str = idempotency_key or order_id_str

    order_insert_sql = text("""
        INSERT INTO orders (id, order_number, user_id, item_id, status, idempotency_key, amount_cents, created_at, updated_at)
        VALUES (:id, :order_number, :user_id, :item_id, 'CONFIRMED', :idempotency_key, :amount_cents, :now, :now)
        ON CONFLICT (idempotency_key) DO NOTHING;
    """)

    stock_update_sql = text("""
        UPDATE items
        SET reserved_stock = reserved_stock + 1,
            updated_at = :now
        WHERE id = :item_id AND reserved_stock < total_stock;
    """)

    audit_log_sql = text("""
        INSERT INTO inventory_audit_log (item_id, order_id, delta, type, notes, created_at)
        VALUES (:item_id, :order_id, -1, 'RESERVATION', 'Asynchronous persistence from worker stream', :now);
    """)

    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(
                    order_insert_sql,
                    {
                        "id": order_id_str,
                        "order_number": order_number,
                        "user_id": user_id_str,
                        "item_id": item_id_str,
                        "idempotency_key": idem_key_str,
                        "amount_cents": amount_cents,
                        "now": now_str,
                    }
                )
                await session.execute(stock_update_sql, {"item_id": item_id_str, "now": now_str})
                await session.execute(
                    audit_log_sql,
                    {"item_id": item_id_str, "order_id": order_id_str, "now": now_str}
                )

        logger.info(f"[DB Persist] Order {order_number} committed successfully.")
        return True

    except IntegrityError as e:
        logger.warning(f"[DB Persist] Idempotency or constraint conflict for order {order_number}: {e}")
        return True  # Handled safely as idempotent no-op
    except Exception as e:
        logger.error(f"[DB Persist] Failed to commit order {order_number}: {e}")
        raise
