"""Background task: fallback processing of expired PENDING ride verifications.

Runs every 5 minutes and resolves rides whose verification_deadline has passed
but were not processed by the ping handler (e.g. Search App is offline).

Uses the same ``process_expired_verifications`` logic as the ping handler to
ensure identical decision-making and double-processing protection via atomic
``UPDATE ... WHERE verification_status = 'PENDING'`` with rowcount check.
"""

import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

from firebase_admin import exceptions as firebase_exceptions
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.ride import Ride
from app.redis import redis_client
from app.services.credit_service import cache_balance
from app.services.fcm_service import send_ride_credit_refunded
from app.services.ping_service import process_expired_verifications

logger = logging.getLogger(__name__)

VERIFICATION_INTERVAL_SECONDS = 5 * 60  # 5 minutes
INITIAL_DELAY_SECONDS = 20  # Stagger startup relative to other tasks


async def get_users_with_expired_rides(db: AsyncSession) -> list[UUID]:
    """Return distinct user_ids that have PENDING rides past their deadline.

    Uses the partial index ``idx_rides_verification`` for efficient lookup.
    """
    now = datetime.now(UTC)
    result = await db.execute(
        select(Ride.user_id)
        .where(
            Ride.verification_status == "PENDING",
            Ride.verification_deadline < now,
        )
        .distinct()
    )
    return list(result.scalars().all())


async def process_user_verifications(user_id: UUID, db: AsyncSession, redis: Redis) -> list[dict]:
    """Process expired ride verifications for one user and handle side effects.

    Delegates to ``process_expired_verifications`` (shared with ping handler),
    then commits the transaction, updates Redis balance cache, and sends FCM
    push notifications for any cancelled rides with refunds.

    Args:
        user_id: User whose expired rides to process.
        db: Database session (will be committed by this function).
        redis: Redis client for balance cache writes.

    Returns:
        List of dicts for each CANCELLED ride that was refunded::

            [{"ride_id": UUID, "credits_refunded": int, "new_balance": int}]
    """
    cancelled_rides = await process_expired_verifications(db, user_id, redis)
    await db.commit()

    if cancelled_rides:
        last_balance = cancelled_rides[-1]["new_balance"]
        await cache_balance(user_id, last_balance, redis)

        for refund_info in cancelled_rides:
            try:
                await send_ride_credit_refunded(
                    db,
                    user_id,
                    refund_info["ride_id"],
                    refund_info["credits_refunded"],
                    refund_info["new_balance"],
                )
            except (firebase_exceptions.FirebaseError, OperationalError):
                logger.warning(
                    "FCM RIDE_CREDIT_REFUNDED failed in verification fallback for ride %s",
                    refund_info["ride_id"],
                    exc_info=True,
                )

    return cancelled_rides


async def run_verification_fallback() -> None:
    """Background task: resolve expired PENDING rides every 5 minutes.

    Fallback for offline Search App devices that aren't pinging the server.
    The ping handler processes expired verifications on each ping; this task
    ensures rides are eventually resolved even when no pings arrive.

    Each user is processed in an isolated DB session so that a failure for
    one user does not affect others.
    """
    logger.info(
        "Ride verification fallback task started (interval=%d seconds)",
        VERIFICATION_INTERVAL_SECONDS,
    )
    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    while True:
        try:
            async with AsyncSessionLocal() as db:
                user_ids = await get_users_with_expired_rides(db)

            if not user_ids:
                logger.debug("Ride verification fallback: no expired rides found")
            else:
                logger.info(
                    "Ride verification fallback: processing %d user(s)",
                    len(user_ids),
                )
                for user_id in user_ids:
                    try:
                        async with AsyncSessionLocal() as db:
                            await process_user_verifications(user_id, db, redis_client)
                    except Exception:
                        logger.exception(
                            "Ride verification fallback error for user %s",
                            user_id,
                        )
        except Exception:
            logger.exception("Ride verification fallback error")

        await asyncio.sleep(VERIFICATION_INTERVAL_SECONDS)
