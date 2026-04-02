import logging
import uuid
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException
from packaging.version import InvalidVersion, Version
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.accept_failure import AcceptFailure as AcceptFailureModel
from app.models.paired_device import PairedDevice
from app.models.ride import Ride
from app.models.search_filters import SearchFilters
from app.schemas.ping import AcceptFailureItem, PingRequest, PingStats, RideStatusReport
from app.services.credit_service import refund_credits_in_txn

logger = logging.getLogger(__name__)

BATCH_DEDUP_TTL = 3600  # 1 hour TTL for batch deduplication
BATCH_KEY_PREFIX = "stats_batch:"
DEFAULT_CYCLE_DURATION_MS = 15000  # Default search cycle duration when not reported
MIN_INTERVAL_SECONDS = 5  # Minimum interval between search cycles
SAFETY_MULTIPLIER = 2  # Safety margin for interval=0 deadline proximity check


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


def calculate_dynamic_interval(
    requests_per_day: int,
    requests_per_hour: list[float],
    local_hour: int,
    cycle_duration_ms: int | None = None,
) -> int:
    """Calculate search interval based on hourly weight distribution.

    Args:
        requests_per_day: Total daily request budget (e.g., 1920).
        requests_per_hour: List of 24 percentage weights (must sum to ~100).
        local_hour: Current hour (0-23) in device local timezone.
        cycle_duration_ms: Last search cycle duration in milliseconds.
            Falls back to DEFAULT_CYCLE_DURATION_MS if None.

    Returns:
        Interval in seconds (integer), minimum MIN_INTERVAL_SECONDS.
    """
    weight = requests_per_hour[local_hour]
    requests_this_hour = weight / 100 * requests_per_day
    total_cycle_time = 3600 / requests_this_hour
    cycle_duration_s = (cycle_duration_ms or DEFAULT_CYCLE_DURATION_MS) / 1000
    interval = total_cycle_time - cycle_duration_s
    return max(int(interval), MIN_INTERVAL_SECONDS)


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
    - app_version: from request
    - accessibility_enabled, lyft_running, screen_on: from device_health (if provided)
    - latitude, longitude, location_updated_at: from location (if provided)
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
    device.app_version = request.app_version

    # Update health fields only if device_health is provided
    if request.device_health is not None:
        if request.device_health.accessibility_enabled is not None:
            device.accessibility_enabled = request.device_health.accessibility_enabled
        if request.device_health.lyft_running is not None:
            device.lyft_running = request.device_health.lyft_running
        if request.device_health.screen_on is not None:
            device.screen_on = request.device_health.screen_on

    # Update location if provided
    if request.location is not None:
        device.latitude = request.location.latitude
        device.longitude = request.location.longitude
        device.location_updated_at = datetime.now(UTC)

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


async def process_ride_status_reports(
    db: AsyncSession,
    user_id: uuid.UUID,
    ride_statuses: list[RideStatusReport] | None,
) -> int:
    """Process ride verification status reports from Search App.

    For each report, finds the matching ride (by ride_hash + user_id)
    and updates:
    - last_reported_present: set to the reported value (true/false)
    - disappeared_at: recorded on first present=false (never overwritten)

    Unknown ride_hashes are silently ignored.

    Args:
        db: Database session (caller is responsible for committing).
        user_id: User UUID who owns the rides.
        ride_statuses: List of RideStatusReport from ping request.

    Returns:
        Number of rides updated.
    """
    if not ride_statuses:
        return 0

    # Build hash → present map (last value wins if duplicates)
    report_map: dict[str, bool] = {r.ride_hash: r.present for r in ride_statuses}

    # Bulk fetch matching rides in one query (uses idx_rides_ride_hash)
    result = await db.execute(
        select(Ride).where(
            Ride.user_id == user_id,
            Ride.ride_hash.in_(list(report_map.keys())),
        )
    )
    rides = result.scalars().all()

    if not rides:
        return 0

    now = datetime.now(UTC)
    updated = 0

    for ride in rides:
        present = report_map[ride.ride_hash]
        ride.last_reported_present = present

        if not present and ride.disappeared_at is None:
            ride.disappeared_at = now

        updated += 1

    return updated


async def process_expired_verifications(
    db: AsyncSession,
    user_id: uuid.UUID,
    redis: Redis,
) -> list[dict]:
    """Process PENDING rides with expired verification_deadline (PRD section 6, step 5).

    For each expired ride, decides based on last_reported_present:
    - true or NULL  -> CONFIRMED  (ride is on track, keep credits)
    - false         -> CANCELLED  (ride disappeared, refund credits)

    Uses atomic ``UPDATE ... WHERE verification_status = 'PENDING'`` with
    rowcount check to prevent double-processing with the background
    ride_verification task.

    For CANCELLED rides with credits_charged > 0, the refund (CreditBalance +
    CreditTransaction) happens in the **same savepoint** as the status change —
    one COMMIT covers both (PRD section 6, "double-processing protection").

    Caller must commit the transaction afterwards and may use the returned info
    to send FCM pushes and update Redis balance cache.

    Args:
        db: Database session (caller controls commit).
        user_id: User whose rides to process.
        redis: Redis client (for balance cache update after commit).

    Returns:
        List of dicts for each CANCELLED ride that was refunded::

            [{"ride_id": UUID, "credits_refunded": int, "new_balance": int}]

        Empty list when nothing was cancelled/refunded.
    """
    now = datetime.now(UTC)

    result = await db.execute(
        select(Ride).where(
            Ride.user_id == user_id,
            Ride.verification_status == "PENDING",
            Ride.verification_deadline < now,
        )
    )
    expired_rides = result.scalars().all()

    if not expired_rides:
        return []

    cancelled_with_refund: list[dict] = []

    for ride in expired_rides:
        # Capture scalar values before the savepoint — sa_update() inside
        # begin_nested() expires ORM instances, making attribute access
        # unsafe after a savepoint rollback.
        ride_id = ride.id
        credits_charged = ride.credits_charged
        new_status = "CANCELLED" if ride.last_reported_present is False else "CONFIRMED"

        try:
            async with db.begin_nested():
                # Atomic status change — double-processing protection.
                # If another process (background task or concurrent ping)
                # already handled this ride, rowcount will be 0.
                stmt = (
                    sa_update(Ride)
                    .where(
                        Ride.id == ride_id,
                        Ride.verification_status == "PENDING",
                    )
                    .values(
                        verification_status=new_status,
                        verified_at=now,
                    )
                )
                update_result = await db.execute(stmt)

                if update_result.rowcount == 0:  # type: ignore[attr-defined]
                    # Another process already handled this ride — skip.
                    logger.debug(
                        "Ride %s already processed by another handler",
                        ride_id,
                    )
                    continue

                # CANCELLED with credits → refund in the same savepoint.
                if new_status == "CANCELLED" and credits_charged > 0:
                    new_balance = await refund_credits_in_txn(
                        user_id=user_id,
                        amount=credits_charged,
                        reference_id=ride_id,
                        db=db,
                    )

                    # Record refunded amount on the ride.
                    await db.execute(
                        sa_update(Ride)
                        .where(Ride.id == ride_id)
                        .values(credits_refunded=credits_charged)
                    )

                    cancelled_with_refund.append(
                        {
                            "ride_id": ride_id,
                            "credits_refunded": credits_charged,
                            "new_balance": new_balance,
                        }
                    )

            # Savepoint committed — log the outcome.
            if new_status == "CONFIRMED":
                logger.info("RIDE_AUTO_CONFIRMED: ride_id=%s", ride_id)
            else:
                logger.info("RIDE_AUTO_CANCELLED: ride_id=%s", ride_id)

        except OperationalError as exc:
            if "could not obtain lock" in str(exc):
                # CreditBalance row is locked by another process.
                # Savepoint was rolled back — ride stays PENDING.
                # Will be retried on next ping or by background task.
                logger.warning(
                    "RIDE_REFUND_LOCK_FAILED: ride_id=%s, will retry",
                    ride_id,
                )
                continue
            raise

    return cancelled_with_refund


def _get_cycle_duration_seconds(
    cycle_duration_ms: int | None,
    last_interval_sent: int | None,
) -> float:
    """Estimate cycle duration in seconds for interval=0 threshold calculation.

    Priority:
    1. cycle_duration_ms from ping request (converted to seconds)
    2. last_interval_sent * 2 from previous ping response (approximation:
       full cycle ≈ interval + search time ≈ interval * 2)
    3. DEFAULT_CYCLE_DURATION_MS / 1000 as final fallback
    """
    if cycle_duration_ms is not None:
        return cycle_duration_ms / 1000
    if last_interval_sent is not None:
        return last_interval_sent * 2
    return DEFAULT_CYCLE_DURATION_MS / 1000


async def build_verify_rides(
    db: AsyncSession,
    user_id: uuid.UUID,
    check_interval_minutes: int,
    cycle_duration_ms: int | None,
    last_interval_sent: int | None,
) -> list[str]:
    """Build throttled list of ride hashes for Search App to verify.

    Selects PENDING rides whose verification_deadline has not yet passed,
    then applies per-ride throttle logic based on check_interval_minutes:

    - interval > 0: include ride if never checked before OR enough time
      has elapsed since last_verification_requested_at.
    - interval == 0: include ride only when it is close to its deadline
      (within cycle_duration * SAFETY_MULTIPLIER seconds).

    Updates last_verification_requested_at for all included rides in a
    single bulk UPDATE.  Caller is responsible for committing.

    Args:
        db: Database session (caller controls commit).
        user_id: User whose rides to check.
        check_interval_minutes: Min minutes between checks per ride.
            0 = check only right before the deadline.
        cycle_duration_ms: Last cycle duration from ping request (ms).
        last_interval_sent: Last interval_seconds sent to device (for fallback).

    Returns:
        List of ride_hash strings for rides that should be verified.
    """
    now = datetime.now(UTC)

    # Fetch PENDING rides whose deadline is still in the future.
    result = await db.execute(
        select(Ride).where(
            Ride.user_id == user_id,
            Ride.verification_status == "PENDING",
            Ride.verification_deadline > now,
        )
    )
    pending_rides = result.scalars().all()

    if not pending_rides:
        return []

    included: list[Ride] = []

    if check_interval_minutes == 0:
        # Special case: check only when close to the deadline.
        cycle_duration_s = _get_cycle_duration_seconds(cycle_duration_ms, last_interval_sent)
        threshold = timedelta(seconds=cycle_duration_s * SAFETY_MULTIPLIER)

        for ride in pending_rides:
            time_until_deadline = ride.verification_deadline - now  # type: ignore[operator]
            if time_until_deadline <= threshold:
                included.append(ride)
    else:
        interval_seconds = check_interval_minutes * 60

        for ride in pending_rides:
            if (
                ride.last_verification_requested_at is None
                or (now - ride.last_verification_requested_at).total_seconds() >= interval_seconds
            ):
                included.append(ride)

    if not included:
        return []

    # Bulk update last_verification_requested_at for included rides.
    ride_ids = [r.id for r in included]
    await db.execute(
        sa_update(Ride).where(Ride.id.in_(ride_ids)).values(last_verification_requested_at=now)
    )

    return [ride.ride_hash for ride in included]
