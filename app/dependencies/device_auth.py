import hashlib
from datetime import UTC, datetime

from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.paired_device import PairedDevice


async def verify_device(
    x_device_token: str = Header(..., alias="X-Device-Token"),
    x_device_id: str = Header(..., alias="X-Device-Id"),
    db: AsyncSession = Depends(get_db),
) -> PairedDevice:
    """Verify device token from headers and return the authenticated PairedDevice.

    Extracts X-Device-Token and X-Device-Id headers, hashes the token
    with SHA256, and looks up the device in the database by both
    token_hash AND device_id. Rejects expired tokens.

    Returns:
        The matching PairedDevice record.

    Raises:
        HTTPException(401): If the token/device_id combination is invalid or expired.
    """
    token_hash = hashlib.sha256(x_device_token.encode()).hexdigest()

    result = await db.execute(
        select(PairedDevice).where(
            PairedDevice.device_token_hash == token_hash,
            PairedDevice.device_id == x_device_id,
        )
    )
    device = result.scalar_one_or_none()

    if not device:
        raise HTTPException(status_code=401, detail="INVALID_DEVICE_TOKEN")

    if device.expires_at is not None and device.expires_at < datetime.now(UTC):
        raise HTTPException(status_code=401, detail="DEVICE_TOKEN_EXPIRED")

    return device
