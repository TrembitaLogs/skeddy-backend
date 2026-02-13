import logging
import uuid
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException
from packaging.version import InvalidVersion, Version
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.accept_failure import AcceptFailure as AcceptFailureModel
from app.models.paired_device import PairedDevice
from app.models.search_filters import SearchFilters
from app.schemas.ping import AcceptFailureItem, PingRequest, PingStats

logger = logging.getLogger(__name__)

BATCH_DEDUP_TTL = 3600  # 1 hour TTL for batch deduplication
BATCH_KEY_PREFIX = "stats_batch:"


def validate_timezone(timezone_str: str) -> ZoneInfo:
    """Validate IANA timezone identifier and return ZoneInfo object.

    Args:
        timezone_str: IANA timezone identifier (e.g., 'America/New_York').

    Returns:
        ZoneInfo object for the given timezone.

    Raises:
        HTTPException(422): If timezone_str is not a valid IANA timezone.
    """
    try:
        return ZoneInfo(timezone_str)
    except (ZoneInfoNotFoundError, KeyError, ValueError):
        raise HTTPException(status_code=422, detail="INVALID_TIMEZONE")


def check_app_version(app_version: str, min_version: str) -> bool:
    """Check if app version meets minimum requirements.

    Uses PEP 440 version comparison via packaging library.

    Args:
        app_version: Version string from client (e.g., '1.2.3').
        min_version: Minimum required version (e.g., '1.0.0').

    Returns:
        True if app_version >= min_version, False otherwise.
        Returns False for invalid version strings (treated as outdated).
    """
    try:
        return Version(app_version) >= Version(min_version)
    except (InvalidVersion, TypeError):
        return False


def parse_time(time_str: str) -> time:
    """Parse HH:MM string to time object.

    Args:
        time_str: Time in HH:MM 24-hour format (e.g., '06:30', '22:00').

    Returns:
        A time object representing the given time.
    """
    hour, minute = map(int, time_str.split(":"))
    return time(hour, minute)


def is_within_schedule(filters: SearchFilters, timezone_str: str) -> bool:
    """Check if current time is within working schedule (DST-safe).

    Handles:
    - 24h mode (working_time >= 24): only check working_days
    - Overnight schedules (e.g., 22:00 start, 10h = until 08:00 next day)
    - DST transitions: uses timezone-aware datetime, not naive time comparisons

    For overnight schedules, the working_day check is performed against
    the day the shift STARTED, not the current day.

    Args:
        filters: SearchFilters with start_time, working_time, working_days.
        timezone_str: IANA timezone identifier.

    Returns:
        True if currently within schedule, False otherwise.
    """
    tz = ZoneInfo(timezone_str)
    now = datetime.now(tz)

    # 24h mode: only check if today is a working day
    if filters.working_time >= 24:
        day_name = now.strftime("%a").upper()[:3]
        return day_name in filters.working_days

    start_time = parse_time(filters.start_time)
    today = now.date()

    # Build today's schedule window (timezone-aware)
    today_start = datetime(
        today.year,
        today.month,
        today.day,
        start_time.hour,
        start_time.minute,
        tzinfo=tz,
    )
    today_end = today_start + timedelta(hours=filters.working_time)

    if today_start <= now < today_end:
        start_day = today_start.strftime("%a").upper()[:3]
        return start_day in filters.working_days

    # Check yesterday's window (for overnight schedules)
    yesterday = today - timedelta(days=1)
    yesterday_start = datetime(
        yesterday.year,
        yesterday.month,
        yesterday.day,
        start_time.hour,
        start_time.minute,
        tzinfo=tz,
    )
    yesterday_end = yesterday_start + timedelta(hours=filters.working_time)

    if yesterday_start <= now < yesterday_end:
        start_day = yesterday_start.strftime("%a").upper()[:3]
        return start_day in filters.working_days

    return False


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


async def update_device_state(
    db: AsyncSession,
    device: PairedDevice,
    request: PingRequest,
    interval_seconds: int | None = None,
) -> None:
    """Update device state with ping data.

    Updates:
    - last_ping_at: current UTC timestamp
    - timezone: from request (validated IANA identifier)
    - accessibility_enabled, lyft_running, screen_on: from device_health (if provided)
    - last_interval_sent: the interval sent in response (if provided)
    - offline_notified: reset to False (device is online)

    Args:
        db: Database session.
        device: PairedDevice model instance.
        request: PingRequest with device state.
        interval_seconds: Interval sent in response (for tracking).
    """
    device.last_ping_at = datetime.now(UTC)
    device.timezone = request.timezone

    # Update health fields only if device_health is provided
    if request.device_health is not None:
        if request.device_health.accessibility_enabled is not None:
            device.accessibility_enabled = request.device_health.accessibility_enabled
        if request.device_health.lyft_running is not None:
            device.lyft_running = request.device_health.lyft_running
        if request.device_health.screen_on is not None:
            device.screen_on = request.device_health.screen_on

    # Track interval for health monitoring
    if interval_seconds is not None:
        device.last_interval_sent = interval_seconds

    # Reset offline notification flag since device is online
    device.offline_notified = False

    await db.commit()
    await db.refresh(device)


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
    except Exception:
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
