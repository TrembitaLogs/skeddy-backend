import logging
import uuid

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.accept_failure import AcceptFailure as AcceptFailureModel
from app.schemas.ping import AcceptFailureItem, PingStats

logger = logging.getLogger(__name__)

BATCH_DEDUP_TTL = settings.BATCH_DEDUP_TTL
BATCH_KEY_PREFIX = "stats_batch:"


async def is_batch_already_processed(redis: Redis, batch_id: str) -> bool:
    """Check if stats batch was already processed.

    Args:
        redis: Async Redis client.
        batch_id: UUID v4 batch identifier from PingStats.

    Returns:
        True if batch was already processed, False otherwise.
    """
    key = f"{BATCH_KEY_PREFIX}{batch_id}"
    return bool(await redis.exists(key))


async def mark_batch_as_processed(redis: Redis, batch_id: str) -> None:
    """Mark stats batch as processed with TTL.

    Args:
        redis: Async Redis client.
        batch_id: UUID v4 batch identifier.
    """
    key = f"{BATCH_KEY_PREFIX}{batch_id}"
    await redis.setex(key, BATCH_DEDUP_TTL, "1")


async def process_stats_if_new(
    redis: Redis,
    stats: PingStats | None,
) -> tuple[bool, list[AcceptFailureItem]]:
    """Process stats only if batch is new (not duplicate).

    On Redis unavailability, skips deduplication and processes stats anyway
    (accepts duplication risk rather than failing the ping).

    Args:
        redis: Async Redis client.
        stats: PingStats from request (may be None).

    Returns:
        Tuple of (was_processed, failures_to_save):
        - was_processed: True if this is a new batch that was processed.
        - failures_to_save: List of AcceptFailureItem to save (empty if duplicate or None).
    """
    if stats is None:
        return (False, [])

    try:
        if await is_batch_already_processed(redis, stats.batch_id):
            return (False, [])

        # Mark as processed BEFORE saving to prevent race conditions
        await mark_batch_as_processed(redis, stats.batch_id)
    except RedisError:
        logger.warning(
            "Redis unavailable during batch deduplication for batch_id=%s, "
            "processing without dedup",
            stats.batch_id,
        )

    return (True, stats.accept_failures)


async def save_accept_failures(
    db: AsyncSession,
    user_id: uuid.UUID,
    failures: list[AcceptFailureItem],
) -> int:
    """Save accept failures to database.

    Uses a savepoint so that failures in saving stats do not affect
    subsequent database operations in the same session.

    Args:
        db: Database session.
        user_id: User UUID who reported the failures.
        failures: List of AcceptFailureItem from PingStats.

    Returns:
        Number of failures saved (0 on error or empty list).
    """
    if not failures:
        return 0

    records = [
        AcceptFailureModel(
            user_id=user_id,
            reason=failure.reason,
            ride_price=failure.ride_price,
            pickup_time=failure.pickup_time,
            reported_at=failure.timestamp,
        )
        for failure in failures
    ]

    try:
        async with db.begin_nested():
            db.add_all(records)
            await db.flush()
    except (OperationalError, IntegrityError):
        logger.error(
            "Failed to save %d accept failures for user %s",
            len(records),
            user_id,
            exc_info=True,
        )
        return 0

    logger.info(
        "Saved %d accept failures for user %s",
        len(records),
        user_id,
    )
    return len(records)
