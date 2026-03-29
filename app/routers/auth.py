import logging
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
from app.models.credit_transaction import CreditTransaction, TransactionType
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
    VerifyEmailRequest,
)
from app.schemas.pairing import SearchLoginRequest, SearchLoginResponse
from app.services.auth_service import (
    create_access_token,
    create_refresh_token,
    delete_reset_code,
    delete_user_refresh_tokens,
    delete_verify_code,
    get_user_by_phone,
    hash_password,
    hash_refresh_token,
    refresh_tokens,
    save_refresh_token,
    store_reset_code,
    store_verify_code,
    verify_password,
    verify_reset_code,
    verify_verify_code,
)
from app.services.credit_service import cache_balance, create_balance_with_bonus
from app.services.email_service import send_password_reset_code, send_verification_code
from app.services.pairing_service import search_login
from app.utils.codes import generate_six_digit_code

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# Pre-computed bcrypt hash for timing-safe login verification.
# When user is not found, we still run bcrypt.checkpw against this
# dummy hash to prevent email enumeration via response time differences.
_DUMMY_HASH = hash_password("timing-safe-dummy")


@router.post("/register", response_model=AuthResponse, status_code=201)
@limiter.limit("10/minute")
async def register(
    request: Request,
    response: Response,
    body: RegisterRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    # Check email uniqueness
    result = await db.execute(select(User).where(User.email == body.email))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="EMAIL_ALREADY_EXISTS")

    # Check phone uniqueness (if provided)
    if body.phone_number:
        existing = await get_user_by_phone(db, body.phone_number)
        if existing:
            raise HTTPException(status_code=409, detail="PHONE_ALREADY_EXISTS")

    # Create user with hashed password
    user = User(
        email=body.email,
        password_hash=hash_password(body.password),
        phone_number=body.phone_number,
    )
    db.add(user)
    await db.flush()  # Get user.id before creating related records

    # Create related records with defaults
    search_filters = SearchFilters(user_id=user.id)
    search_status = SearchStatus(user_id=user.id)
    db.add_all([search_filters, search_status])

    # Create credit balance with registration bonus (flush only, no commit)
    credit_balance = await create_balance_with_bonus(user.id, db, redis)

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
    except IntegrityError as exc:
        await db.rollback()
        detail = "PHONE_ALREADY_EXISTS" if "phone_number" in str(exc) else "EMAIL_ALREADY_EXISTS"
        raise HTTPException(status_code=409, detail=detail)

    # Write-through Redis cache for credit balance (after commit per PRD)
    await cache_balance(user.id, credit_balance.balance, redis)

    # Send verification email (failure must not break registration)
    code = generate_six_digit_code()
    try:
        await store_verify_code(redis, str(user.id), code)
        await send_verification_code(body.email, code)
    except Exception:
        logger.warning("Failed to send verification email for user %s", user.id)

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
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    has_legacy_claim = await db.execute(
        select(CreditTransaction.id)
        .where(
            CreditTransaction.user_id == current_user.id,
            CreditTransaction.type == TransactionType.LEGACY_IMPORT,
        )
        .limit(1)
    )
    return ProfileResponse(
        user_id=current_user.id,
        email=current_user.email,
        email_verified=current_user.email_verified,
        phone_number=current_user.phone_number,
        license_number=current_user.license_number,
        legacy_credits_claimed=has_legacy_claim.scalar_one_or_none() is not None,
        created_at=current_user.created_at,
    )


@router.post("/verify-email", response_model=OkResponse)
@limiter.limit("10/minute", key_func=get_user_key)
async def verify_email(
    request: Request,
    response: Response,
    body: VerifyEmailRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    # Check if already verified
    if current_user.email_verified:
        raise HTTPException(status_code=400, detail="ALREADY_VERIFIED")

    # Verify Redis availability
    try:
        await redis.ping()  # type: ignore[misc]
    except RedisError:
        logger.error("Redis unavailable for email verification")
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    # Verify code (raises 401 on failure)
    await verify_verify_code(redis, str(current_user.id), body.code)

    # Mark email as verified
    current_user.email_verified = True
    await db.commit()

    # Delete used code from Redis
    await delete_verify_code(redis, str(current_user.id))

    logger.info("Email verified for user %s", current_user.id)
    return OkResponse()


@router.post("/resend-verification", response_model=OkResponse)
@limiter.limit("3/hour", key_func=get_user_key)
async def resend_verification(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    redis: Redis = Depends(get_redis),
):
    # Check if already verified
    if current_user.email_verified:
        raise HTTPException(status_code=400, detail="ALREADY_VERIFIED")

    # Verify Redis availability
    try:
        await redis.ping()  # type: ignore[misc]
    except RedisError:
        logger.error("Redis unavailable for resend verification")
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    # Generate new verification code (overwrites previous in Redis)
    code = generate_six_digit_code()
    await store_verify_code(redis, str(current_user.id), code)

    # Send verification email (failure must not break the endpoint)
    try:
        await send_verification_code(current_user.email, code)
    except Exception:
        logger.warning("Failed to send verification email for user %s", current_user.id)

    return OkResponse()


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
        code = generate_six_digit_code()
        await store_reset_code(redis, body.email, code)

        try:
            await send_password_reset_code(body.email, code)
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
    # Verify Redis availability - reset codes are stored in Redis
    try:
        await redis.ping()  # type: ignore[misc]
    except RedisError:
        logger.error("Redis unavailable for password reset")
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    # Verify reset code (raises 401 on failure)
    await verify_reset_code(redis, body.email, body.code)

    # Find user by email
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()
    if user is None:
        # Code was valid but user doesn't exist (edge case: deleted between request-reset and reset)
        await delete_reset_code(redis, body.email)
        raise HTTPException(status_code=401, detail="INVALID_RESET_CODE")

    user.password_hash = hash_password(body.new_password)

    # Invalidate all refresh tokens (force re-login on all devices)
    await delete_user_refresh_tokens(db, user.id)

    # Delete used reset code from Redis
    await delete_reset_code(redis, body.email)

    logger.info("Password reset completed for user %s", user.id)
    return OkResponse()


@router.post("/search-login", response_model=SearchLoginResponse)
@limiter.limit("5/minute")
async def search_login_endpoint(
    request: Request,
    response: Response,
    body: SearchLoginRequest,
    db: AsyncSession = Depends(get_db),
):
    """Authenticate Search App via email/password and register the device.

    No JWT auth required — credentials are provided in the request body.
    On success, returns a long-lived device_token for subsequent device auth.
    Logging in on a new device replaces the old one (old device gets 401 on next ping).
    """
    device_token, user_id = await search_login(
        email=body.email,
        password=body.password,
        device_id=body.device_id,
        timezone_str=body.timezone,
        db=db,
        device_model=body.device_model,
    )
    return SearchLoginResponse(device_token=device_token, user_id=user_id)
