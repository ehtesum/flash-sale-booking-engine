from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID
import jwt
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from shared.config import settings
from shared.schemas import UserClaims

security = HTTPBearer(auto_error=False)

DEMO_PERSONAS = {
    "alice": {
        "user_id": UUID("550e8400-e29b-41d4-a716-446655440001"),
        "email": "alice@example.com",
        "name": "Alice Rahman",
        "role": "customer",
        "tier": "REGULAR",
    },
    "bob": {
        "user_id": UUID("550e8400-e29b-41d4-a716-446655440002"),
        "email": "bob@example.com",
        "name": "Bob Chen",
        "role": "customer",
        "tier": "REGULAR",
    },
    "charlie": {
        "user_id": UUID("550e8400-e29b-41d4-a716-446655440003"),
        "email": "charlie@example.com",
        "name": "Charlie Al-Mansoor",
        "role": "customer",
        "tier": "VIP",
    },
    "admin": {
        "user_id": UUID("550e8400-e29b-41d4-a716-446655440099"),
        "email": "admin@flashsale.internal",
        "name": "SRE Administrator",
        "role": "admin",
        "tier": "ADMIN",
    },
}

def create_jwt_token(claims: UserClaims, expires_delta: Optional[timedelta] = None) -> str:
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta or timedelta(minutes=settings.JWT_EXPIRATION_MINUTES))
    payload = {
        "sub": str(claims.user_id),
        "email": claims.email,
        "name": claims.name,
        "role": claims.role,
        "tier": claims.tier,
        "exp": expire,
        "iat": now,
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

async def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Security(security)) -> UserClaims:
    """Validates JWT signature without database queries during peak traffic."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header. Bearer token required.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
        return UserClaims(
            user_id=UUID(payload["sub"]),
            email=payload["email"],
            name=payload["name"],
            role=payload.get("role", "customer"),
            tier=payload.get("tier", "REGULAR"),
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token has expired.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token signature.",
            headers={"WWW-Authenticate": "Bearer"},
        )
