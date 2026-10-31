from datetime import datetime, timezone
from typing import Optional, Any, Dict, List
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class ProblemDetails(BaseModel):
    """RFC 7807 specification for HTTP error responses."""
    type: str = Field(..., description="URI reference identifying problem category")
    title: str = Field(..., description="Short summary of error")
    status: int = Field(..., description="HTTP status code")
    detail: str = Field(..., description="Human-readable explanation of error")
    instance: str = Field(..., description="URI reference identifying request path")
    code: str = Field(..., description="Application-specific error code")
    timestamp: datetime = Field(default_factory=utc_now)
    invalid_params: Optional[List[Dict[str, Any]]] = None

class UserClaims(BaseModel):
    user_id: UUID
    email: str
    name: str
    role: str = "customer"
    tier: str = "REGULAR"

class DemoLoginRequest(BaseModel):
    persona: str = Field("alice", description="Persona identifier: 'alice', 'bob', 'charlie', or 'admin'")

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: UUID
    name: str
    tier: str

class ItemSchema(BaseModel):
    id: UUID
    sku: str
    title: str
    category: str
    total_stock: int
    reserved_stock: int
    available_stock: int
    price_cents: int
    sale_start: datetime
    sale_end: datetime

class ReservationRequest(BaseModel):
    item_id: UUID

class ReservationResponse(BaseModel):
    order_id: UUID
    order_number: str
    item_id: UUID
    user_id: UUID
    status: str = "RESERVED"
    message: str = "Stock secured. Order persisting asynchronously."
    check_url: str

class OrderSchema(BaseModel):
    id: UUID
    order_number: str
    item_id: UUID
    user_id: UUID
    status: str
    amount_cents: int
    created_at: datetime
    updated_at: datetime

class OrderReservedEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    order_number: str
    item_id: UUID
    user_id: UUID
    amount_cents: int
    idempotency_key: Optional[str] = None
    correlation_id: str
    timestamp: datetime = Field(default_factory=utc_now)

class OrderFailedEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    item_id: UUID
    user_id: UUID
    reason: str
    error_details: Optional[Dict[str, Any]] = None
    correlation_id: str
    failed_at: datetime = Field(default_factory=utc_now)
