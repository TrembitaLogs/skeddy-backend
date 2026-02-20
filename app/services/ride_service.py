import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ride import Ride
from app.models.user import User

logger = logging.getLogger(__name__)


async def get_ride_by_idempotency(
    db: AsyncSession, user_id: UUID, idempotency_key: str
) -> Ride | None:
    """Look up an existing ride by user_id and idempotency_key.

    Uses the unique index idx_rides_idempotency for efficient lookup.

    Args:
        db: Async database session.
        user_id: The user's UUID.
        idempotency_key: Client-generated UUID for deduplication.

    Returns:
        Existing Ride if found, None otherwise.
    """
    result = await db.execute(
        select(Ride).where(
            Ride.user_id == user_id,
            Ride.idempotency_key == idempotency_key,
        )
    )
    return result.scalar_one_or_none()


async def create_ride(
    db: AsyncSession,
    user_id: UUID,
    idempotency_key: str,
    event_type: str,
    ride_data: dict,
) -> Ride:
    """Create a new ride event and flush to the database.

    The caller is responsible for committing the transaction.

    Args:
        db: Async database session.
        user_id: The user's UUID.
        idempotency_key: Client-generated UUID for deduplication.
        event_type: Event type (e.g., "ACCEPTED").
        ride_data: Ride data as dict (stored as JSONB).

    Returns:
        The newly created Ride instance with id populated.

    Raises:
        IntegrityError: If a ride with the same (user_id, idempotency_key)
            already exists (concurrent insert race condition).
    """
    ride = Ride(
        user_id=user_id,
        idempotency_key=idempotency_key,
        event_type=event_type,
        ride_data=ride_data,
    )
    db.add(ride)
    await db.flush()
    return ride


async def get_user_ride_events(
    db: AsyncSession,
    user_id: UUID,
    limit: int,
    offset: int,
    since: datetime | None = None,
) -> tuple[list[Ride], int]:
    """Get paginated ride events for a user, ordered by created_at descending.

    Uses the idx_rides_user_created index for efficient queries.

    Args:
        db: Async database session.
        user_id: The user's UUID.
        limit: Maximum number of events to return.
        offset: Number of events to skip.
        since: If provided, only return events created at or after this time.

    Returns:
        Tuple of (list of Ride events, total count).
    """
    filters = [Ride.user_id == user_id]
    if since is not None:
        filters.append(Ride.created_at >= since)

    count_result = await db.execute(select(func.count()).select_from(Ride).where(*filters))
    total = count_result.scalar_one()

    result = await db.execute(
        select(Ride).where(*filters).order_by(Ride.created_at.desc()).offset(offset).limit(limit)
    )
    events = list(result.scalars().all())

    return events, total


async def get_user_fcm_token(db: AsyncSession, user_id: UUID) -> str | None:
    """Get the FCM token for a user.

    Args:
        db: Async database session.
        user_id: The user's UUID.

    Returns:
        The user's FCM token, or None if not set.
    """
    result = await db.execute(select(User.fcm_token).where(User.id == user_id))
    return result.scalar_one_or_none()
