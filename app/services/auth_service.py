import hashlib
import json
import logging
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
# reset_code:{email} → JSON {"code_hash": "sha256...", "attempts": 0}, TTL: 15 min
_RESET_CODE_PREFIX = "reset_code:"
_RESET_CODE_TTL = 900  # 15 minutes
_RESET_CODE_MAX_ATTEMPTS = 5

# --- Email verification code Redis key schema (code-based) ---
# verify_code:{user_id} → JSON {"code_hash": "sha256...", "attempts": 0}, TTL: 24 hours
_VERIFY_CODE_PREFIX = "verify_code:"
_VERIFY_CODE_TTL = 86400  # 24 hours
_VERIFY_CODE_MAX_ATTEMPTS = 5


def hash_password(password: str) -> str:
    salt = bcrypt.gensalt(rounds=_ROUNDS)
    return bcrypt.hashpw(password.encode(), salt).decode()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def create_access_token(user_id: UUID) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
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
        _RESET_CODE_TTL,
        json.dumps({"code_hash": code_hash, "attempts": 0}),
    )


async def verify_reset_code(redis: Redis, email: str, code: str) -> bool:
    """Verify a 6-digit password reset code against the stored hash.

    Increments the attempt counter on each wrong guess.  After
    ``_RESET_CODE_MAX_ATTEMPTS`` wrong attempts the code is invalidated.

    Returns ``True`` when the code is valid.

    Raises ``HTTPException(401)`` with ``INVALID_RESET_CODE`` when:
    - no code exists for this email (expired or never requested)
    - the code is wrong
    - the attempt limit has been reached
    """
    key = f"{_RESET_CODE_PREFIX}{email}"
    raw = await redis.get(key)
    if raw is None:
        raise HTTPException(status_code=401, detail="INVALID_RESET_CODE")

    data = json.loads(raw)

    # Check attempt limit first
    if data["attempts"] >= _RESET_CODE_MAX_ATTEMPTS:
        await redis.delete(key)
        raise HTTPException(status_code=401, detail="INVALID_RESET_CODE")

    code_hash = hashlib.sha256(code.encode()).hexdigest()

    if code_hash != data["code_hash"]:
        # Increment attempts, preserve remaining TTL
        data["attempts"] += 1
        ttl = await redis.ttl(key)
        if ttl > 0:
            await redis.setex(key, ttl, json.dumps(data))
        else:
            # Key is about to expire anyway - just delete
            await redis.delete(key)
        raise HTTPException(status_code=401, detail="INVALID_RESET_CODE")

    return True


async def delete_reset_code(redis: Redis, email: str) -> None:
    """Delete a reset code from Redis after successful password reset."""
    await redis.delete(f"{_RESET_CODE_PREFIX}{email}")


async def store_verify_code(redis: Redis, user_id: str, code: str) -> None:
    """Store an email verification code hash in Redis with 24-hour TTL.

    Any previous unused code for the same user is implicitly overwritten
    because the key ``verify_code:{user_id}`` is the same.
    """
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    await redis.setex(
        f"{_VERIFY_CODE_PREFIX}{user_id}",
        _VERIFY_CODE_TTL,
        json.dumps({"code_hash": code_hash, "attempts": 0}),
    )


async def verify_verify_code(redis: Redis, user_id: str, code: str) -> bool:
    """Verify a 6-digit email verification code against the stored hash.

    Increments the attempt counter on each wrong guess.  After
    ``_VERIFY_CODE_MAX_ATTEMPTS`` wrong attempts the code is invalidated.

    Returns ``True`` when the code is valid.

    Raises ``HTTPException(401)`` with ``INVALID_VERIFICATION_CODE`` when:
    - no code exists for this user (expired or never requested)
    - the code is wrong
    - the attempt limit has been reached
    """
    key = f"{_VERIFY_CODE_PREFIX}{user_id}"
    raw = await redis.get(key)
    if raw is None:
        raise HTTPException(status_code=401, detail="INVALID_VERIFICATION_CODE")

    data = json.loads(raw)

    # Check attempt limit first
    if data["attempts"] >= _VERIFY_CODE_MAX_ATTEMPTS:
        await redis.delete(key)
        raise HTTPException(status_code=401, detail="INVALID_VERIFICATION_CODE")

    code_hash = hashlib.sha256(code.encode()).hexdigest()

    if code_hash != data["code_hash"]:
        # Increment attempts, preserve remaining TTL
        data["attempts"] += 1
        ttl = await redis.ttl(key)
        if ttl > 0:
            await redis.setex(key, ttl, json.dumps(data))
        else:
            # Key is about to expire anyway - just delete
            await redis.delete(key)
        raise HTTPException(status_code=401, detail="INVALID_VERIFICATION_CODE")

    return True


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
