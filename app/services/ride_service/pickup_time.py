import logging
import re
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

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

    # Weekday: "Mon", "Tue", "Wednesday", etc. -- match first 3 chars
    abbrev = lower[:3]
    if abbrev in _DAY_ABBREVS:
        target_weekday = _DAY_ABBREVS[abbrev]
        current_weekday = today_date.weekday()
        days_ahead = (target_weekday - current_weekday) % 7
        # If same day of week, Lyft would say "Today" -- so this means next week
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

    Supported formats (with separators: ' . ', ' - ', '.'):
      - 'Today . 6:05AM'
      - 'Tomorrow . 3:30PM'
      - 'Mon . 10:00AM' (next occurrence of that weekday)
      - 'Feb 25 . 2:00PM' (month and day)

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
        logger.debug("Pickup time parse failed: no separator found in %r", pickup_time_str)
        return None

    date_str, time_str = parts[0].strip(), parts[1].strip()

    time_result = _parse_time_part(time_str)
    if time_result is None:
        logger.debug("Pickup time parse failed: invalid time part %r", time_str)
        return None
    hour, minute = time_result

    now_local = datetime.now(tz)
    target_date = _resolve_date(date_str, now_local)
    if target_date is None:
        logger.debug("Pickup time parse failed: unrecognized date part %r", date_str)
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
