import hashlib
import json
import logging
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from uuid import UUID

import bcrypt
import jwt
from fastapi import HTTPException
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.refresh_token import RefreshToken
from app.models.user import User

logger = logging.getLogger(__name__)

_ROUNDS = 12

# --- Password reset code Redis key schema (code-based) ---
# reset_code:{email} → JSON {"code_hash": "sha256...", "attempts": 0}
_RESET_CODE_PREFIX = "reset_code:"

# --- Email verification code Redis key schema (code-based) ---
# verify_code:{user_id} → JSON {"code_hash": "sha256...", "attempts": 0}
_VERIFY_CODE_PREFIX = "verify_code:"


def hash_password(password: str) -> str:
    salt = bcrypt.gensalt(rounds=_ROUNDS)
    return bcrypt.hashpw(password.encode(), salt).decode()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def create_access_token(user_id: UUID) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
        "jti": str(uuid.uuid4()),
        "exp": now + timedelta(hours=settings.JWT_ACCESS_TOKEN_EXPIRE_HOURS),
        "iat": now,
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def decode_access_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


# --- Access token blacklist Redis key schema ---
# blacklist:{jti} → "1", TTL = remaining token lifetime
_BLACKLIST_PREFIX = "blacklist:"


async def blacklist_access_token(redis: Redis, token: str) -> None:
    """Add an access token's JTI to the Redis blacklist.

    The key expires when the token would have expired naturally,
    so no manual cleanup is needed.
    """
    payload = decode_access_token(token)
    if not payload:
        return
    jti = payload.get("jti")
    if not jti:
        return
    exp = payload.get("exp", 0)
    remaining = int(exp - datetime.now(UTC).timestamp())
    if remaining <= 0:
        return
    await redis.setex(f"{_BLACKLIST_PREFIX}{jti}", remaining, "1")


async def is_token_blacklisted(redis: Redis, jti: str) -> bool:
    """Check whether a JTI is present in the blacklist."""
    result: int = await redis.exists(f"{_BLACKLIST_PREFIX}{jti}")
    return result > 0


def create_refresh_token() -> str:
    return str(uuid.uuid4())


def hash_refresh_token(token: str) -> str:
    """Hash a refresh token using SHA256."""
    return hashlib.sha256(token.encode()).hexdigest()


async def save_refresh_token(
    db: AsyncSession, user_id: UUID, token: str, expires_at: datetime
) -> RefreshToken:
    """Hash and save a refresh token to the database."""
    token_hash = hash_refresh_token(token)
    refresh_token = RefreshToken(user_id=user_id, token_hash=token_hash, expires_at=expires_at)
    db.add(refresh_token)
    await db.commit()
    await db.refresh(refresh_token)
    return refresh_token


async def get_refresh_token_by_hash(db: AsyncSession, token_hash: str) -> RefreshToken | None:
    """Find a refresh token record by its SHA256 hash."""
    result = await db.execute(select(RefreshToken).where(RefreshToken.token_hash == token_hash))
    return result.scalar_one_or_none()


async def delete_refresh_token(db: AsyncSession, token_hash: str) -> None:
    """Delete a single refresh token by its hash."""
    await db.execute(delete(RefreshToken).where(RefreshToken.token_hash == token_hash))
    await db.commit()


async def delete_user_refresh_tokens(db: AsyncSession, user_id: UUID) -> int:
    """Delete all refresh tokens for a user. Returns the number of deleted tokens."""
    result = await db.execute(delete(RefreshToken).where(RefreshToken.user_id == user_id))
    await db.commit()
    return result.rowcount  # type: ignore[no-any-return, attr-defined]


async def store_reset_code(redis: Redis, email: str, code: str) -> None:
    """Store a password reset code hash in Redis with 15-minute TTL.

    Any previous unused code for the same email is implicitly overwritten
    because the key ``reset_code:{email}`` is the same.
    """
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    await redis.setex(
        f"{_RESET_CODE_PREFIX}{email}",
        settings.RESET_CODE_TTL,
        json.dumps({"code_hash": code_hash, "attempts": 0}),
    )


async def _verify_code(
    redis: Redis, key: str, code: str, *, max_attempts: int, error_detail: str
) -> bool:
    """Verify a 6-digit code against the stored hash.

    Shared logic for both password-reset and email-verification flows.
    Increments the attempt counter on each wrong guess.  After
    ``max_attempts`` wrong attempts the code is invalidated.

    Returns ``True`` when the code is valid.
    Raises ``HTTPException(401)`` with the given ``error_detail`` on failure.
    """
    raw = await redis.get(key)
    if raw is None:
        raise HTTPException(status_code=401, detail=error_detail)

    data = json.loads(raw)

    if data["attempts"] >= max_attempts:
        await redis.delete(key)
        raise HTTPException(status_code=401, detail=error_detail)

    code_hash = hashlib.sha256(code.encode()).hexdigest()

    if not secrets.compare_digest(code_hash, data["code_hash"]):
        data["attempts"] += 1
        ttl = await redis.ttl(key)
        if ttl > 0:
            await redis.setex(key, ttl, json.dumps(data))
        else:
            await redis.delete(key)
        raise HTTPException(status_code=401, detail=error_detail)

    return True


async def verify_reset_code(redis: Redis, email: str, code: str) -> bool:
    """Verify a 6-digit password reset code against the stored hash."""
    return await _verify_code(
        redis,
        f"{_RESET_CODE_PREFIX}{email}",
        code,
        max_attempts=settings.RESET_CODE_MAX_ATTEMPTS,
        error_detail="INVALID_RESET_CODE",
    )


async def delete_reset_code(redis: Redis, email: str) -> None:
    """Delete a reset code from Redis after successful password reset."""
    await redis.delete(f"{_RESET_CODE_PREFIX}{email}")


async def store_verify_code(
    redis: Redis, user_id: str, code: str, *, new_email: str | None = None
) -> None:
    """Store an email verification code hash in Redis with 24-hour TTL.

    Any previous unused code for the same user is implicitly overwritten
    because the key ``verify_code:{user_id}`` is the same.

    If ``new_email`` is provided, it is stored alongside the code so that
    ``verify-email`` can apply the email change upon successful verification.
    """
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    data: dict = {"code_hash": code_hash, "attempts": 0}
    if new_email is not None:
        data["new_email"] = new_email
    await redis.setex(
        f"{_VERIFY_CODE_PREFIX}{user_id}",
        settings.VERIFY_CODE_TTL,
        json.dumps(data),
    )


async def verify_verify_code(redis: Redis, user_id: str, code: str) -> bool:
    """Verify a 6-digit email verification code against the stored hash."""
    return await _verify_code(
        redis,
        f"{_VERIFY_CODE_PREFIX}{user_id}",
        code,
        max_attempts=settings.VERIFY_CODE_MAX_ATTEMPTS,
        error_detail="INVALID_VERIFICATION_CODE",
    )


async def delete_verify_code(redis: Redis, user_id: str) -> None:
    """Delete a verification code from Redis after successful verification."""
    await redis.delete(f"{_VERIFY_CODE_PREFIX}{user_id}")


async def get_user_by_phone(db: AsyncSession, phone_number: str) -> User | None:
    """Find a user by phone number."""
    result = await db.execute(select(User).where(User.phone_number == phone_number))
    return result.scalar_one_or_none()


async def refresh_tokens(db: AsyncSession, redis: Redis, old_refresh_token: str) -> dict:
    """Refresh token pair with Redis grace period for concurrent requests.

    If the same old token arrives again within 10s, returns the cached result
    from Redis instead of failing with 401. When Redis is unavailable, grace
    period is skipped (acceptable degradation).
    """
    old_hash = hash_refresh_token(old_refresh_token)
    cache_key = f"refresh_grace:{old_hash}"

    # Step 1: Check Redis grace cache first
    try:
        cached = await redis.get(cache_key)
        if cached:
            return dict(json.loads(cached))
    except RedisError:
        logger.warning("Redis unavailable during grace period check")

    # Step 2: Validate token in DB
    token_record = await get_refresh_token_by_hash(db, old_hash)
    if not token_record:
        raise HTTPException(status_code=401, detail="INVALID_REFRESH_TOKEN")
    if token_record.expires_at < datetime.now(UTC):
        await delete_refresh_token(db, old_hash)
        raise HTTPException(status_code=401, detail="REFRESH_TOKEN_EXPIRED")

    # Extract user_id before delete commits (avoids expired instance after commit)
    user_id = token_record.user_id

    # Step 3: Generate new token pair
    new_access = create_access_token(user_id)
    new_refresh = create_refresh_token()

    # Step 4: Rotate tokens in DB
    await delete_refresh_token(db, old_hash)
    await save_refresh_token(
        db,
        user_id,
        new_refresh,
        datetime.now(UTC) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS),
    )

    # Step 5: Cache response with 10s TTL for grace period
    response = {
        "user_id": str(user_id),
        "access_token": new_access,
        "refresh_token": new_refresh,
    }
    try:
        await redis.setex(cache_key, 10, json.dumps(response))
    except RedisError:
        logger.warning("Redis unavailable during grace period cache write")

    return response
