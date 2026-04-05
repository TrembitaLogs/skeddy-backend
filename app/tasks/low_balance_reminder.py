"""Background task: daily low-balance push notifications.

Runs once every 24 hours and sends FCM CREDITS_LOW notifications to users
whose credit balance is above zero but below the maximum ride cost in credits
(PRD section 8).

Anti-spam: Redis key ``low_balance_notified:{user_id}`` (TTL 24 h) prevents
repeated reminders.  The key is cleared by ``credit_service.add_credits``
when the balance rises above the threshold after a purchase.
"""

import asyncio
import logging
from uuid import UUID

from firebase_admin import exceptions as firebase_exceptions
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.credit_balance import CreditBalance
from app.redis import redis_client
from app.services.credit_service import (
    get_max_ride_credits,
    low_balance_notified_key,
)
from app.services.fcm_service import send_credits_low

logger = logging.getLogger(__name__)

REMINDER_INTERVAL_SECONDS = 86400  # 24 hours
INITIAL_DELAY_SECONDS = 7200  # 2 h stagger from other daily tasks

BATCH_SIZE = 100
BATCH_PAUSE_SECONDS = 0.05  # 50 ms between batches

LOW_BALANCE_NOTIFIED_TTL = 86400  # 24 hours


async def get_low_balance_users(threshold: int, db: AsyncSession) -> list[tuple[UUID, int]]:
    """Return (user_id, balance) pairs where 0 < balance < *threshold*.

    Uses the partial index ``idx_credit_balances_low`` (WHERE balance > 0).
    """
    result = await db.execute(
        select(CreditBalance.user_id, CreditBalance.balance).where(
            CreditBalance.balance > 0,
            CreditBalance.balance < threshold,
        )
    )
    return [(row.user_id, row.balance) for row in result.all()]


async def process_user(
    user_id: UUID,
    balance: int,
    threshold: int,
    db: AsyncSession,
    redis: Redis,
) -> bool:
    """Send CREDITS_LOW push to a single user if not already notified.

    Returns True if push was sent, False if skipped (already notified or
    Redis unavailable — errs on the side of not spamming).
    """
    redis_key = low_balance_notified_key(user_id)

    # Check anti-spam flag
    try:
        already_notified = await redis.get(redis_key)
        if already_notified is not None:
            return False
    except RedisError:
        # Redis down — skip this user to avoid duplicate notifications.
        # Next cycle (24 h later) will retry.
        logger.warning(
            "Redis unavailable checking %s, skipping user %s",
            redis_key,
            user_id,
        )
        return False

    # Send FCM push (fire-and-forget inside send_credits_low)
    await send_credits_low(db, user_id, balance, threshold)

    # Set anti-spam flag
    try:
        await redis.setex(redis_key, LOW_BALANCE_NOTIFIED_TTL, "1")
    except RedisError:
        # Push already sent but flag not persisted — acceptable.
        # Worst case: user gets one extra reminder next cycle.
        logger.warning("Redis unavailable setting %s for user %s", redis_key, user_id)

    return True


async def run_low_balance_reminder() -> None:
    """Background task: send CREDITS_LOW reminders once every 24 hours.

    Finds users with 0 < balance < max_ride_credits and sends FCM push
    notifications with anti-spam protection via Redis.
    """
    logger.info(
        "Low balance reminder task started (interval=%d seconds)",
        REMINDER_INTERVAL_SECONDS,
    )
    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    while True:
        try:
            # Determine threshold from AppConfig
            async with AsyncSessionLocal() as db:
                threshold = await get_max_ride_credits(db, redis_client)

            # Fetch all eligible users
            async with AsyncSessionLocal() as db:
                users = await get_low_balance_users(threshold, db)

            if not users:
                logger.debug("Low balance reminder: no users below threshold")
            else:
                logger.info(
                    "Low balance reminder: processing %d user(s) (threshold=%d credits)",
                    len(users),
                    threshold,
                )

                sent_count = 0
                for i in range(0, len(users), BATCH_SIZE):
                    batch = users[i : i + BATCH_SIZE]
                    for user_id, balance in batch:
                        try:
                            async with AsyncSessionLocal() as db:
                                sent = await process_user(
                                    user_id,
                                    balance,
                                    threshold,
                                    db,
                                    redis_client,
                                )
                            if sent:
                                sent_count += 1
                        except (
                            OperationalError,
                            RedisError,
                            firebase_exceptions.FirebaseError,
                            OSError,
                        ):
                            logger.exception(
                                "Low balance reminder error for user %s",
                                user_id,
                            )

                    # Pause between batches to avoid FCM rate limiting
                    if i + BATCH_SIZE < len(users):
                        await asyncio.sleep(BATCH_PAUSE_SECONDS)

                logger.info(
                    "Low balance reminder complete: sent %d notification(s)",
                    sent_count,
                )

        except (OperationalError, RedisError, OSError):
            logger.exception("Low balance reminder task error")

        await asyncio.sleep(REMINDER_INTERVAL_SECONDS)
