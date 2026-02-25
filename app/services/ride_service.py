import logging
import re
from datetime import UTC, date, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ride import Ride
from app.models.user import User

logger = logging.getLogger(__name__)

# Pickup time parsing helpers
_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(AM|PM)", re.IGNORECASE)
_MONTH_DAY_RE = re.compile(r"([A-Za-z]+)\s+(\d{1,2})$")
_SEPARATORS = (" · ", " - ", "·")

_DAY_ABBREVS = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}
_MONTH_ABBREVS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _parse_time_part(time_str: str) -> tuple[int, int] | None:
    """Parse '6:05AM' or '3:30PM' into (hour_24, minute).

    Returns None if the string doesn't match the expected pattern.
    """
    m = _TIME_RE.match(time_str)
    if not m:
        return None
    hour = int(m.group(1))
    minute = int(m.group(2))
    ampm = m.group(3).upper()

    if hour < 1 or hour > 12 or minute > 59:
        return None

    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0

    return (hour, minute)


def _resolve_date(date_str: str, today: datetime) -> date | None:
    """Resolve the date portion of a Lyft pickup string to a date.

    Handles: 'Today', 'Tomorrow', weekday names ('Mon', 'Tuesday'),
    and month+day ('Feb 25', 'Mar 2').

    Args:
        date_str: The date portion (already stripped).
        today: Current local datetime (timezone-aware) for reference.

    Returns:
        A date object, or None if parsing fails.
    """
    today_date = today.date()
    lower = date_str.lower().strip()

    if lower == "today":
        return today_date

    if lower == "tomorrow":
        return today_date + timedelta(days=1)

    # Weekday: "Mon", "Tue", "Wednesday", etc. — match first 3 chars
    abbrev = lower[:3]
    if abbrev in _DAY_ABBREVS:
        target_weekday = _DAY_ABBREVS[abbrev]
        current_weekday = today_date.weekday()
        days_ahead = (target_weekday - current_weekday) % 7
        # If same day of week, Lyft would say "Today" — so this means next week
        if days_ahead == 0:
            days_ahead = 7
        return today_date + timedelta(days=days_ahead)

    # Month + day: "Feb 25", "Mar 2"
    m = _MONTH_DAY_RE.match(date_str.strip())
    if m:
        month_str = m.group(1).lower()[:3]
        day = int(m.group(2))
        month = _MONTH_ABBREVS.get(month_str)
        if month is None:
            return None
        year = today_date.year
        try:
            target = date(year, month, day)
        except ValueError:
            return None
        # If this date is in the past, assume next year
        if target < today_date:
            try:
                target = date(year + 1, month, day)
            except ValueError:
                return None
        return target

    return None


def parse_pickup_time(pickup_time_str: str, tz: ZoneInfo) -> datetime | None:
    """Parse a Lyft Driver pickup_time string into a UTC datetime.

    Supported formats (with separators: ' · ', ' - ', '·'):
      - 'Today · 6:05AM'
      - 'Tomorrow · 3:30PM'
      - 'Mon · 10:00AM' (next occurrence of that weekday)
      - 'Feb 25 · 2:00PM' (month and day)

    Args:
        pickup_time_str: Raw string from Lyft Driver UI.
        tz: ZoneInfo for the driver's local timezone.

    Returns:
        Timezone-aware datetime in UTC, or None if parsing fails.
    """
    parts = None
    for sep in _SEPARATORS:
        if sep in pickup_time_str:
            parts = pickup_time_str.split(sep, 1)
            break

    if parts is None or len(parts) != 2:
        return None

    date_str, time_str = parts[0].strip(), parts[1].strip()

    time_result = _parse_time_part(time_str)
    if time_result is None:
        return None
    hour, minute = time_result

    now_local = datetime.now(tz)
    target_date = _resolve_date(date_str, now_local)
    if target_date is None:
        return None

    try:
        local_dt = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            hour,
            minute,
            tzinfo=tz,
        )
    except (ValueError, OverflowError):
        return None

    return local_dt.astimezone(ZoneInfo("UTC"))


def calculate_verification_deadline(pickup_dt: datetime | None, deadline_minutes: int) -> datetime:
    """Calculate verification deadline from parsed pickup datetime.

    If pickup_dt is None (parsing failed) or the calculated deadline
    is already in the past, returns now (UTC).

    Args:
        pickup_dt: Parsed pickup time in UTC, or None.
        deadline_minutes: Minutes before pickup for verification deadline.

    Returns:
        Verification deadline as a UTC datetime.
    """
    now_utc = datetime.now(UTC)

    if pickup_dt is None:
        return now_utc

    deadline = pickup_dt - timedelta(minutes=deadline_minutes)
    if deadline < now_utc:
        return now_utc

    return deadline


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
    ride_hash: str,
    verification_deadline: datetime | None = None,
) -> Ride:
    """Create a new ride event and flush to the database.

    The caller is responsible for committing the transaction.

    Args:
        db: Async database session.
        user_id: The user's UUID.
        idempotency_key: Client-generated UUID for deduplication.
        event_type: Event type (e.g., "ACCEPTED").
        ride_data: Ride data as dict (stored as JSONB).
        ride_hash: SHA-256 hash of ride fields (64 hex chars) for verification matching.
        verification_deadline: UTC datetime for verification cutoff, or None.

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
        ride_hash=ride_hash,
        verification_deadline=verification_deadline,
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


_EVENTS_CUTOFF_WEEKS = 8


async def get_unified_events(
    db: AsyncSession,
    user_id: UUID,
    limit: int,
    cursor: tuple[datetime, str, UUID] | None = None,
    since: datetime | None = None,
) -> tuple[list, bool]:
    """Get unified event feed combining rides and credit transactions.

    Executes a UNION ALL query with per-branch LIMIT optimization
    (PRD section 11).  Rides include billing fields; credit events
    exclude RIDE_CHARGE and RIDE_REFUND (those are embedded in ride
    cards via credits_charged / credits_refunded).

    Each returned row has the following named columns:
        event_kind, id, created_at, event_type, ride_data,
        credits_charged, credits_refunded, verification_status,
        credit_type, amount, balance_after, description.
    Unused columns are NULL depending on event_kind.

    Args:
        db: Async database session.
        user_id: The user's UUID.
        limit: Maximum number of events to return.
        cursor: Decoded cursor tuple (created_at, event_kind, event_id),
            or None for the first page.
        since: If provided, only return events created after this time.
            Combined with the 8-week cutoff via max().

    Returns:
        Tuple of (list of Row objects, has_more flag).
    """
    cutoff_8_weeks = datetime.now(UTC) - timedelta(weeks=_EVENTS_CUTOFF_WEEKS)

    if since is not None:
        effective_cutoff = max(since, cutoff_8_weeks)
    else:
        effective_cutoff = cutoff_8_weeks

    fetch_limit = limit + 1  # +1 to detect has_more
    params: dict = {
        "user_id": user_id,
        "effective_cutoff": effective_cutoff,
        "branch_limit": fetch_limit,
        "fetch_limit": fetch_limit,
    }

    cursor_clause_ride = ""
    cursor_clause_credit = ""
    if cursor is not None:
        cursor_ts, cursor_kind, cursor_id = cursor
        params["cursor_ts"] = cursor_ts
        params["cursor_kind"] = cursor_kind
        params["cursor_id"] = cursor_id
        cursor_clause_ride = (
            "AND (created_at, 'ride', id) < (:cursor_ts, :cursor_kind, :cursor_id)"
        )
        cursor_clause_credit = (
            "AND (created_at, 'credit', id) < (:cursor_ts, :cursor_kind, :cursor_id)"
        )

    sql = text(f"""
        WITH ride_events AS (
            SELECT
                'ride'::text AS event_kind,
                id,
                created_at,
                event_type,
                ride_data,
                credits_charged,
                credits_refunded,
                verification_status,
                NULL::varchar(30) AS credit_type,
                NULL::integer AS amount,
                NULL::integer AS balance_after,
                NULL::varchar(500) AS description
            FROM rides
            WHERE user_id = :user_id
              AND created_at > :effective_cutoff
              {cursor_clause_ride}
            ORDER BY created_at DESC, id DESC
            LIMIT :branch_limit
        ),
        credit_events AS (
            SELECT
                'credit'::text AS event_kind,
                id,
                created_at,
                NULL::varchar(20) AS event_type,
                NULL::jsonb AS ride_data,
                NULL::integer AS credits_charged,
                NULL::integer AS credits_refunded,
                NULL::varchar(20) AS verification_status,
                type AS credit_type,
                amount,
                balance_after,
                description
            FROM credit_transactions
            WHERE user_id = :user_id
              AND type NOT IN ('RIDE_CHARGE', 'RIDE_REFUND')
              AND created_at > :effective_cutoff
              {cursor_clause_credit}
            ORDER BY created_at DESC, id DESC
            LIMIT :branch_limit
        )
        SELECT * FROM (
            SELECT * FROM ride_events
            UNION ALL
            SELECT * FROM credit_events
        ) combined
        ORDER BY created_at DESC, event_kind DESC, id DESC
        LIMIT :fetch_limit
    """)

    result = await db.execute(sql, params)
    rows = list(result.fetchall())

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    return rows, has_more
