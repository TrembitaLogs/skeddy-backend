import hashlib
import logging
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies.auth import get_current_user
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.refresh_token import RefreshToken
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User
from app.redis import get_redis
from app.schemas.auth import (
    AuthResponse,
    ChangePasswordRequest,
    DeleteAccountRequest,
    LoginRequest,
    OkResponse,
    ProfileResponse,
    RefreshRequest,
    RegisterRequest,
    RequestResetRequest,
    ResetPasswordRequest,
    UpdatePhoneRequest,
)
from app.services.auth_service import (
    create_access_token,
    create_refresh_token,
    delete_reset_token,
    delete_user_refresh_tokens,
    get_user_by_phone,
    hash_password,
    hash_refresh_token,
    refresh_tokens,
    save_refresh_token,
    store_reset_token,
    verify_password,
    verify_reset_token,
)
from app.services.email_service import send_reset_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# Pre-computed bcrypt hash for timing-safe login verification.
# When user is not found, we still run bcrypt.checkpw against this
# dummy hash to prevent email enumeration via response time differences.
_DUMMY_HASH = hash_password("timing-safe-dummy")


@router.post("/register", response_model=AuthResponse, status_code=201)
@limiter.limit("10/minute")
async def register(
    request: Request, response: Response, body: RegisterRequest, db: AsyncSession = Depends(get_db)
):
    # Check email uniqueness
    result = await db.execute(select(User).where(User.email == body.email))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="EMAIL_ALREADY_EXISTS")

    # Create user with hashed password
    user = User(email=body.email, password_hash=hash_password(body.password))
    db.add(user)
    await db.flush()  # Get user.id before creating related records

    # Create related records with defaults
    search_filters = SearchFilters(user_id=user.id)
    search_status = SearchStatus(user_id=user.id)
    db.add_all([search_filters, search_status])

    # Generate tokens
    access_token = create_access_token(user.id)
    refresh_token = create_refresh_token()

    # Prepare refresh token for DB storage (not using save_refresh_token
    # to avoid its internal commit — everything must commit together)
    token_hash = hash_refresh_token(refresh_token)
    expires_at = datetime.now(UTC) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
    db.add(RefreshToken(user_id=user.id, token_hash=token_hash, expires_at=expires_at))

    # Single commit for the entire operation
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="EMAIL_ALREADY_EXISTS")

    return AuthResponse(user_id=user.id, access_token=access_token, refresh_token=refresh_token)


@router.post("/login", response_model=AuthResponse)
@limiter.limit("10/minute")
async def login(
    request: Request, response: Response, body: LoginRequest, db: AsyncSession = Depends(get_db)
):
    # Find user by email
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    # Always run bcrypt verify to prevent timing-based email enumeration
    password_valid = verify_password(body.password, user.password_hash if user else _DUMMY_HASH)
    if not user or not password_valid:
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    # Generate token pair
    access_token = create_access_token(user.id)
    refresh_token = create_refresh_token()
    await save_refresh_token(
        db,
        user.id,
        refresh_token,
        datetime.now(UTC) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS),
    )

    return AuthResponse(user_id=user.id, access_token=access_token, refresh_token=refresh_token)


@router.post("/refresh", response_model=AuthResponse)
@limiter.limit("10/minute")
async def refresh(
    request: Request,
    response: Response,
    body: RefreshRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    result = await refresh_tokens(db, redis, body.refresh_token)
    return AuthResponse(**result)


@router.get("/me", response_model=ProfileResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def get_profile(
    request: Request, response: Response, current_user: User = Depends(get_current_user)
):
    return ProfileResponse(
        user_id=current_user.id,
        email=current_user.email,
        phone_number=current_user.phone_number,
        created_at=current_user.created_at,
    )


@router.post("/change-password", response_model=OkResponse)
@limiter.limit("10/minute")
async def change_password(
    request: Request,
    response: Response,
    body: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not verify_password(body.current_password, current_user.password_hash):
        raise HTTPException(status_code=401, detail="INVALID_CURRENT_PASSWORD")

    current_user.password_hash = hash_password(body.new_password)
    await delete_user_refresh_tokens(db, current_user.id)
    return OkResponse()


@router.post("/logout", response_model=OkResponse)
@limiter.limit("10/minute")
async def logout(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await delete_user_refresh_tokens(db, current_user.id)
    return OkResponse()


@router.put("/phone", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def update_phone(
    request: Request,
    response: Response,
    body: UpdatePhoneRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if body.phone_number is not None:
        existing = await get_user_by_phone(db, body.phone_number)
        if existing and existing.id != current_user.id:
            raise HTTPException(status_code=409, detail="PHONE_ALREADY_EXISTS")

    current_user.phone_number = body.phone_number
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="PHONE_ALREADY_EXISTS")
    return OkResponse()


@router.delete("/account", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def delete_account(
    request: Request,
    response: Response,
    body: DeleteAccountRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not verify_password(body.password, current_user.password_hash):
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    await db.delete(current_user)
    await db.commit()

    logger.info("Account deleted for user %s", current_user.id)
    return OkResponse()


@router.post("/request-reset", response_model=OkResponse)
@limiter.limit("3/minute")
async def request_reset(
    request: Request,
    response: Response,
    body: RequestResetRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    # Verify Redis availability - reset tokens require Redis storage
    try:
        await redis.ping()  # type: ignore[misc]
    except RedisError:
        logger.error("Redis unavailable for password reset request")
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    if user:
        # Generate reset token and its SHA256 hash
        reset_token = str(uuid.uuid4())
        token_hash = hashlib.sha256(reset_token.encode()).hexdigest()

        await store_reset_token(redis, user.id, token_hash)

        try:
            await send_reset_email(body.email, reset_token)
        except Exception:
            logger.warning("Failed to send reset email for password reset request")

    logger.info("Password reset requested")

    # Always return 200 to prevent email enumeration
    return OkResponse()


@router.post("/reset-password", response_model=OkResponse)
@limiter.limit("10/minute")
async def reset_password(
    request: Request,
    response: Response,
    body: ResetPasswordRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    # Verify Redis availability - reset tokens are stored in Redis
    try:
        await redis.ping()  # type: ignore[misc]
    except RedisError:
        logger.error("Redis unavailable for password reset")
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    token_hash = hashlib.sha256(body.token.encode()).hexdigest()

    # Verify token exists in Redis and get associated user_id
    user_id = await verify_reset_token(redis, token_hash)
    if user_id is None:
        raise HTTPException(status_code=401, detail="INVALID_RESET_TOKEN")

    # Delete reset token from Redis (one-time use)
    await delete_reset_token(redis, token_hash, user_id)

    # Update user password
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="INVALID_RESET_TOKEN")

    user.password_hash = hash_password(body.new_password)

    # Invalidate all refresh tokens (force re-login on all devices)
    await delete_user_refresh_tokens(db, user_id)

    logger.info("Password reset completed for user %s", user_id)
    return OkResponse()
