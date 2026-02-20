import hashlib
import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from fastapi import HTTPException
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.accept_failure import AcceptFailure
from app.models.paired_device import PairedDevice
from app.services.ping_service import validate_timezone
from app.utils.codes import generate_six_digit_code

logger = logging.getLogger(__name__)

PAIRING_CODE_TTL = 300  # 5 minutes


async def generate_pairing_code(redis: Redis, user_id: UUID) -> tuple[str, datetime]:
    """Generate a 6-digit pairing code and store it in Redis with 5-minute TTL.

    Invalidates any previous unused code for the same user before generating
    a new one. Stores a reverse mapping ``user_pairing:{user_id}`` → code so
    that the old code can be looked up and deleted on subsequent calls.

    Returns:
        Tuple of (code, expires_at).

    Raises:
        HTTPException(503): If Redis is unavailable.
    """
    try:
        # Invalidate previous code for this user (if any)
        reverse_key = f"user_pairing:{user_id}"
        old_code = await redis.get(reverse_key)
        if old_code:
            await redis.delete(f"pairing_code:{old_code}")

        code = generate_six_digit_code()

        # Store code → user_id mapping
        await redis.setex(f"pairing_code:{code}", PAIRING_CODE_TTL, str(user_id))

        # Store reverse mapping user_id → code (for future invalidation)
        await redis.setex(reverse_key, PAIRING_CODE_TTL, code)

        expires_at = datetime.now(UTC) + timedelta(seconds=PAIRING_CODE_TTL)
        return code, expires_at

    except RedisError as exc:
        logger.error("Redis unavailable during pairing code generation: %s", exc)
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE") from exc


# ---------------------------------------------------------------------------
# confirm_pairing helpers
# ---------------------------------------------------------------------------


async def get_device_by_device_id(db: AsyncSession, device_id: str) -> PairedDevice | None:
    """Find a paired device by its hardware device_id."""
    result = await db.execute(select(PairedDevice).where(PairedDevice.device_id == device_id))
    return result.scalar_one_or_none()


async def get_device_by_user_id(db: AsyncSession, user_id: UUID) -> PairedDevice | None:
    """Find a paired device by owner user_id."""
    result = await db.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    return result.scalar_one_or_none()


async def delete_paired_device(db: AsyncSession, device_pk: UUID) -> None:
    """Delete a paired device by its primary key. Does NOT commit."""
    await db.execute(delete(PairedDevice).where(PairedDevice.id == device_pk))


async def delete_accept_failures(db: AsyncSession, user_id: UUID) -> None:
    """Delete all accept failure records for a user. Does NOT commit."""
    await db.execute(delete(AcceptFailure).where(AcceptFailure.user_id == user_id))


async def create_paired_device(
    db: AsyncSession,
    user_id: UUID,
    device_id: str,
    token_hash: str,
    tz: str,
    device_model: str | None = None,
) -> PairedDevice:
    """Create a new paired device record. Does NOT commit."""
    device = PairedDevice(
        user_id=user_id,
        device_id=device_id,
        device_model=device_model,
        device_token_hash=token_hash,
        timezone=tz,
    )
    db.add(device)
    await db.flush()
    return device


# ---------------------------------------------------------------------------
# confirm_pairing
# ---------------------------------------------------------------------------


async def confirm_pairing(
    code: str,
    device_id: str,
    timezone_str: str,
    redis: Redis,
    db: AsyncSession,
    device_model: str | None = None,
) -> tuple[str, UUID]:
    """Confirm pairing: validate code, clean up old devices, create new pairing.

    Returns:
        Tuple of (device_token, user_id).

    Raises:
        HTTPException(422): If timezone is invalid (INVALID_TIMEZONE).
        HTTPException(404): If pairing code is invalid or expired (PAIRING_CODE_EXPIRED).
        HTTPException(409): If pairing code was already used (PAIRING_CODE_USED).
        HTTPException(503): If Redis is unavailable.
    """
    # 1. Validate timezone (IANA format)
    validate_timezone(timezone_str)

    # 2. Get user_id from Redis, check used codes, delete code
    try:
        user_id_str = await redis.get(f"pairing_code:{code}")
        if not user_id_str:
            # Check if code was already used
            if await redis.exists(f"used_pairing_code:{code}"):
                raise HTTPException(status_code=409, detail="PAIRING_CODE_USED")
            raise HTTPException(status_code=404, detail="PAIRING_CODE_EXPIRED")
        await redis.delete(f"pairing_code:{code}")
        # Mark code as used (same TTL) to detect replay attempts
        await redis.setex(f"used_pairing_code:{code}", PAIRING_CODE_TTL, "1")
    except RedisError as exc:
        logger.error("Redis unavailable during pairing confirmation: %s", exc)
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE") from exc

    user_id = UUID(user_id_str)

    # 3. If device_id already paired to another user, delete old pairing
    existing_device = await get_device_by_device_id(db, device_id)
    if existing_device:
        await delete_paired_device(db, existing_device.id)

    # 4. Delete old device for this user if exists (1 user = 1 device)
    old_device = await get_device_by_user_id(db, user_id)
    if old_device:
        await delete_paired_device(db, old_device.id)
        await delete_accept_failures(db, user_id)

    # 5. Generate device token with SHA256 hash
    device_token = str(uuid4())
    token_hash = hashlib.sha256(device_token.encode()).hexdigest()

    # 6. Create new paired device record
    await create_paired_device(db, user_id, device_id, token_hash, timezone_str, device_model)
    await db.commit()

    return device_token, user_id
