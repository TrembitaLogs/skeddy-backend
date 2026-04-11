import json
import logging
from datetime import UTC, datetime, timedelta

import aiosmtplib
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies.auth import get_current_user, security
from app.dependencies.redis import require_redis
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.credit_transaction import CreditTransaction, TransactionType
from app.models.refresh_token import RefreshToken
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.models.user import User
from app.redis import get_redis
from app.schemas.auth import (
    AuthResponse,
    ChangeEmailRequest,
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
    blacklist_access_token,
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
from app.services.email_service import (
    send_email_change_code,
    send_password_reset_code,
    send_verification_code,
)
from app.services.pairing_service import search_login
from app.utils.codes import generate_six_digit_code

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# NOTE: `request: Request` and `response: Response` parameters appear in every endpoint
# because slowapi's @limiter.limit() decorator requires `request` for key extraction
# and `response` for injecting rate-limit headers (X-RateLimit-*, Retry-After).

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
        await send_verification_code(body.email, code, user.language, db, redis)
    except (aiosmtplib.SMTPException, RedisError, OSError):
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
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    if not user or not password_valid:
        logger.warning(
            "Login failed: invalid credentials",
            extra={"email": body.email, "ip": client_ip, "user_agent": user_agent},
        )
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

    logger.info(
        "Login successful",
        extra={"user_id": str(user.id), "ip": client_ip, "user_agent": user_agent},
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
        legacy_credits_restored=has_legacy_claim.scalar_one_or_none() is not None,
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
    redis: Redis = Depends(require_redis),
):
    # Read stored data to check for pending email change
    key = f"verify_code:{current_user.id}"
    raw = await redis.get(key)
    pending_email = None
    if raw:
        data = json.loads(raw)
        pending_email = data.get("new_email")

    # For initial verification (no pending email change), check if already verified
    if not pending_email and current_user.email_verified:
        raise HTTPException(status_code=400, detail="ALREADY_VERIFIED")

    # Verify code (raises 401 on failure)
    await verify_verify_code(redis, str(current_user.id), body.code)

    if pending_email:
        # Email change flow — update email, keep verified
        current_user.email = pending_email
        current_user.email_verified = True
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            raise HTTPException(status_code=409, detail="EMAIL_ALREADY_EXISTS")
        logger.info("Email changed for user %s to %s", current_user.id, pending_email)
    else:
        # Initial verification flow
        current_user.email_verified = True
        await db.commit()
        logger.info("Email verified for user %s", current_user.id)

    # Delete used code from Redis
    await delete_verify_code(redis, str(current_user.id))

    return OkResponse()


@router.post("/change-email", response_model=OkResponse)
@limiter.limit("3/hour", key_func=get_user_key)
async def change_email(
    request: Request,
    response: Response,
    body: ChangeEmailRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(require_redis),
):
    """Request email change. Sends a verification code to the new email address.

    The email is only updated after the code is confirmed via POST /auth/verify-email.
    """
    # Verify password
    if not verify_password(body.password, current_user.password_hash):
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    # Check if new email is the same as current
    if body.new_email == current_user.email:
        raise HTTPException(status_code=400, detail="EMAIL_UNCHANGED")

    # Check if new email is already taken
    result = await db.execute(select(User).where(User.email == body.new_email))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="EMAIL_ALREADY_EXISTS")

    # Store verification code with pending new_email
    code = generate_six_digit_code()
    await store_verify_code(redis, str(current_user.id), code, new_email=body.new_email)

    # Send verification code to the NEW email address
    try:
        await send_email_change_code(body.new_email, code, current_user.language, db, redis)
    except (aiosmtplib.SMTPException, RedisError, OSError):
        logger.warning("Failed to send email change verification for user %s", current_user.id)

    return OkResponse()


@router.post("/resend-verification", response_model=OkResponse)
@limiter.limit("3/hour", key_func=get_user_key)
async def resend_verification(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    redis: Redis = Depends(require_redis),
):
    # Check if already verified
    if current_user.email_verified:
        raise HTTPException(status_code=400, detail="ALREADY_VERIFIED")

    # Generate new verification code (overwrites previous in Redis)
    code = generate_six_digit_code()
    await store_verify_code(redis, str(current_user.id), code)

    # Send verification email (failure must not break the endpoint)
    try:
        await send_verification_code(
            current_user.email, code, current_user.language, db=None, redis=redis
        )
    except (aiosmtplib.SMTPException, RedisError, OSError):
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
    current_user.password_changed_at = datetime.now(UTC)
    await db.commit()
    await delete_user_refresh_tokens(db, current_user.id)
    return OkResponse()


@router.post("/logout", response_model=OkResponse)
@limiter.limit("10/minute")
async def logout(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer()),
):
    await delete_user_refresh_tokens(db, current_user.id)
    try:
        await blacklist_access_token(redis, credentials.credentials)
    except RedisError:
        logger.warning("Redis unavailable during logout token blacklist — skipping")
    return OkResponse()


@router.delete("/account", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def delete_account(
    request: Request,
    response: Response,
    body: DeleteAccountRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(require_redis),
):
    if not verify_password(body.password, current_user.password_hash):
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    # Blacklist the current access token so it cannot be reused after deletion
    await blacklist_access_token(redis, credentials.credentials)

    await db.delete(current_user)
    await db.commit()

    logger.info("Account deleted for user %s", current_user.id)
    return OkResponse()


@router.post("/request-reset", response_model=OkResponse)
@limiter.limit("1/minute")
async def request_reset(
    request: Request,
    response: Response,
    body: RequestResetRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(require_redis),
):
    result = await db.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()

    if user:
        code = generate_six_digit_code()
        await store_reset_code(redis, body.email, code)

        try:
            await send_password_reset_code(body.email, code, user.language, db, redis)
        except (aiosmtplib.SMTPException, RedisError, OSError):
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
    redis: Redis = Depends(require_redis),
):
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
    user.password_changed_at = datetime.now(UTC)
    await db.commit()

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
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    try:
        device_token, user_id = await search_login(
            email=body.email,
            password=body.password,
            device_id=body.device_id,
            timezone_str=body.timezone,
            db=db,
            device_model=body.device_model,
        )
    except HTTPException:
        logger.warning(
            "Search login failed",
            extra={"email": body.email, "ip": client_ip, "user_agent": user_agent},
        )
        raise
    logger.info(
        "Search login successful",
        extra={"user_id": str(user_id), "ip": client_ip, "user_agent": user_agent},
    )
    return SearchLoginResponse(device_token=device_token, user_id=user_id)
