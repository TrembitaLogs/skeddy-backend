import hashlib
import logging
import secrets
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.accept_failure import AcceptFailure
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.services.auth_service import verify_password
from app.services.ping_service import validate_timezone

logger = logging.getLogger(__name__)

# Pre-computed bcrypt hash for timing-safe login verification.
# When user is not found, we still run bcrypt.checkpw against this
# dummy hash to prevent email enumeration via response time differences.
_DUMMY_HASH: str | None = None


def _get_dummy_hash() -> str:
    """Lazy-init the dummy hash to avoid import-time bcrypt call."""
    global _DUMMY_HASH
    if _DUMMY_HASH is None:
        from app.services.auth_service import hash_password

        _DUMMY_HASH = hash_password("timing-safe-dummy")
    return _DUMMY_HASH


# ---------------------------------------------------------------------------
# Device CRUD helpers
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
# search_login — email/password auth for Search App
# ---------------------------------------------------------------------------


async def search_login(
    email: str,
    password: str,
    device_id: str,
    timezone_str: str,
    db: AsyncSession,
    device_model: str | None = None,
) -> tuple[str, UUID]:
    """Authenticate user with email/password and register the search device.

    Handles device replacement: if the user already has a paired device with a
    different device_id, the old record is deleted. If the device_id is already
    registered to another user, that old record is also deleted.

    Returns:
        Tuple of (device_token, user_id).

    Raises:
        HTTPException(422): If timezone is invalid (INVALID_TIMEZONE).
        HTTPException(401): If credentials are invalid (INVALID_CREDENTIALS).
    """
    # 1. Validate timezone (IANA format)
    validate_timezone(timezone_str)

    # 2. Authenticate user (email + password)
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    # Always run bcrypt verify to prevent timing-based email enumeration
    password_valid = verify_password(password, user.password_hash if user else _get_dummy_hash())
    if not user or not password_valid:
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    # 3. If device_id already paired to another user, delete old pairing
    existing_device = await get_device_by_device_id(db, device_id)
    if existing_device and existing_device.user_id != user.id:
        await delete_paired_device(db, existing_device.id)

    # 4. Delete old device for this user if exists (1 user = 1 device)
    old_device = await get_device_by_user_id(db, user.id)
    if old_device:
        await delete_paired_device(db, old_device.id)
        await delete_accept_failures(db, user.id)

    # 5. Generate device token with SHA256 hash
    device_token = secrets.token_urlsafe(48)
    token_hash = hashlib.sha256(device_token.encode()).hexdigest()

    # 6. Create new paired device record
    await create_paired_device(db, user.id, device_id, token_hash, timezone_str, device_model)
    await db.commit()

    return device_token, user.id
