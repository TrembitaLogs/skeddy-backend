import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update

from app.database import AsyncSessionLocal
from app.models.accept_failure import AcceptFailure
from app.models.credit_transaction import CreditTransaction
from app.models.ride import Ride

logger = logging.getLogger(__name__)

# Run once per day (86400 seconds).
CLEANUP_INTERVAL_SECONDS = 86400

# Offset from startup to avoid running simultaneously with token cleanup.
INITIAL_DELAY_SECONDS = 3600

# Data older than 8 weeks is deleted.
RETENTION_WEEKS = 8

# Delete in batches to avoid long-running locks.
BATCH_SIZE = 1000


async def clear_ride_reference_ids(db, cutoff: datetime) -> int:
    """Set reference_id to NULL in credit_transactions for rides about to be deleted.

    Must be called before delete_old_rides to prevent orphaned references.
    Uses a single UPDATE with subselect as per PRD specification.
    Returns the number of cleared reference_ids.
    """
    stmt = (
        update(CreditTransaction)
        .where(CreditTransaction.reference_id.in_(select(Ride.id).where(Ride.created_at < cutoff)))
        .values(reference_id=None)
    )
    result = await db.execute(stmt)
    await db.commit()
    return result.rowcount  # type: ignore[no-any-return]


async def delete_old_rides(db, cutoff: datetime) -> int:
    """Delete rides created before *cutoff* in batches.

    Returns the total number of deleted rows.
    """
    total_deleted = 0
    while True:
        id_stmt = select(Ride.id).where(Ride.created_at < cutoff).limit(BATCH_SIZE)
        id_result = await db.execute(id_stmt)
        ids = id_result.scalars().all()
        if not ids:
            break
        del_stmt = delete(Ride).where(Ride.id.in_(ids))
        del_result = await db.execute(del_stmt)
        await db.commit()
        total_deleted += del_result.rowcount
    return total_deleted


async def delete_old_accept_failures(db, cutoff: datetime) -> int:
    """Delete accept failures reported before *cutoff* in batches.

    Returns the total number of deleted rows.
    """
    total_deleted = 0
    while True:
        id_stmt = (
            select(AcceptFailure.id).where(AcceptFailure.reported_at < cutoff).limit(BATCH_SIZE)
        )
        id_result = await db.execute(id_stmt)
        ids = id_result.scalars().all()
        if not ids:
            break
        del_stmt = delete(AcceptFailure).where(AcceptFailure.id.in_(ids))
        del_result = await db.execute(del_stmt)
        await db.commit()
        total_deleted += del_result.rowcount
    return total_deleted


async def cleanup_old_data() -> None:
    """Background task that deletes old rides and accept failures once per day.

    Runs in an infinite loop with a 24-hour sleep interval.
    Uses a 1-hour initial delay to avoid running simultaneously with token cleanup.
    """
    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    logger.info(
        "Data cleanup task started (interval=%d seconds, retention=%d weeks)",
        CLEANUP_INTERVAL_SECONDS,
        RETENTION_WEEKS,
    )

    while True:
        try:
            cutoff = datetime.now(UTC) - timedelta(weeks=RETENTION_WEEKS)
            async with AsyncSessionLocal() as db:
                cleared_refs = await clear_ride_reference_ids(db, cutoff)
                deleted_rides = await delete_old_rides(db, cutoff)
                deleted_failures = await delete_old_accept_failures(db, cutoff)

                if cleared_refs > 0 or deleted_rides > 0 or deleted_failures > 0:
                    logger.info(
                        "Data cleanup: cleared %d reference(s), deleted %d ride(s), %d accept failure(s)",
                        cleared_refs,
                        deleted_rides,
                        deleted_failures,
                    )
                else:
                    logger.debug("Data cleanup: no old records found")
        except Exception:
            logger.exception("Data cleanup error")

        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
