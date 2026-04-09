import logging
import uuid
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.ride import Ride, VerificationStatus
from app.schemas.ping import RideStatusReport
from app.services.credit_service import refund_credits_in_txn

logger = logging.getLogger(__name__)

DEFAULT_CYCLE_DURATION_MS = settings.DEFAULT_CYCLE_DURATION_MS
SAFETY_MULTIPLIER = settings.SAFETY_MULTIPLIER


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

    # Build hash -> present map (last value wins if duplicates)
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
    CreditTransaction) happens in the **same savepoint** as the status change --
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
            Ride.verification_status == VerificationStatus.PENDING,
            Ride.verification_deadline < now,
        )
    )
    expired_rides = result.scalars().all()

    if not expired_rides:
        return []

    cancelled_with_refund: list[dict] = []

    for ride in expired_rides:
        # Capture scalar values before the savepoint -- sa_update() inside
        # begin_nested() expires ORM instances, making attribute access
        # unsafe after a savepoint rollback.
        ride_id = ride.id
        credits_charged = ride.credits_charged
        new_status = (
            VerificationStatus.CANCELLED
            if ride.last_reported_present is False
            else VerificationStatus.CONFIRMED
        )

        try:
            async with db.begin_nested():
                # Atomic status change -- double-processing protection.
                # If another process (background task or concurrent ping)
                # already handled this ride, rowcount will be 0.
                stmt = (
                    sa_update(Ride)
                    .where(
                        Ride.id == ride_id,
                        Ride.verification_status == VerificationStatus.PENDING,
                    )
                    .values(
                        verification_status=new_status,
                        verified_at=now,
                    )
                )
                update_result = await db.execute(stmt)

                if update_result.rowcount == 0:  # type: ignore[attr-defined]
                    # Another process already handled this ride -- skip.
                    logger.debug(
                        "Ride %s already processed by another handler",
                        ride_id,
                    )
                    continue

                # CANCELLED with credits -> refund in the same savepoint.
                if new_status == VerificationStatus.CANCELLED and credits_charged > 0:
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

            # Savepoint committed -- log the outcome.
            if new_status == VerificationStatus.CONFIRMED:
                logger.info("RIDE_AUTO_CONFIRMED: ride_id=%s", ride_id)
            else:
                logger.info("RIDE_AUTO_CANCELLED: ride_id=%s", ride_id)

        except OperationalError as exc:
            if "could not obtain lock" in str(exc):
                # CreditBalance row is locked by another process.
                # Savepoint was rolled back -- ride stays PENDING.
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
       full cycle = interval + search time = interval * 2)
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
            Ride.verification_status == VerificationStatus.PENDING,
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
