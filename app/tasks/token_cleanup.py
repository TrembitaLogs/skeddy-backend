import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import delete
from sqlalchemy.exc import OperationalError

from app.database import AsyncSessionLocal
from app.models.paired_device import PairedDevice
from app.models.refresh_token import RefreshToken

logger = logging.getLogger(__name__)

# Run once per day (86400 seconds).
CLEANUP_INTERVAL_SECONDS = 86400


async def delete_expired_refresh_tokens(db) -> int:
    """Delete all refresh tokens whose expires_at is in the past.

    Returns the number of deleted rows.
    """
    now_utc = datetime.now(UTC)
    stmt = delete(RefreshToken).where(RefreshToken.expires_at < now_utc)
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount  # type: ignore[no-any-return]


async def delete_expired_device_tokens(db) -> int:
    """Delete all paired devices whose expires_at is in the past.

    Returns the number of deleted rows.
    """
    now_utc = datetime.now(UTC)
    stmt = delete(PairedDevice).where(
        PairedDevice.expires_at.isnot(None),
        PairedDevice.expires_at < now_utc,
    )
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount  # type: ignore[no-any-return]


async def cleanup_expired_tokens() -> None:
    """Background task that deletes expired refresh and device tokens once per day.

    Runs in an infinite loop with a 24-hour sleep interval.
    An initial delay staggers startup so that multiple cleanup tasks
    do not all fire at the same instant.
    """
    # Small initial delay to stagger background tasks at startup.
    await asyncio.sleep(10)

    logger.info(
        "Token cleanup task started (interval=%d seconds)",
        CLEANUP_INTERVAL_SECONDS,
    )

    while True:
        try:
            async with AsyncSessionLocal() as db:
                deleted = await delete_expired_refresh_tokens(db)
                if deleted > 0:
                    logger.info("Token cleanup: deleted %d expired refresh token(s)", deleted)
                else:
                    logger.debug("Token cleanup: no expired refresh tokens found")
        except OperationalError:
            logger.exception("Token cleanup error (refresh tokens)")

        try:
            async with AsyncSessionLocal() as db:
                deleted = await delete_expired_device_tokens(db)
                if deleted > 0:
                    logger.info("Token cleanup: deleted %d expired device token(s)", deleted)
                else:
                    logger.debug("Token cleanup: no expired device tokens found")
        except OperationalError:
            logger.exception("Token cleanup error (device tokens)")

        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
