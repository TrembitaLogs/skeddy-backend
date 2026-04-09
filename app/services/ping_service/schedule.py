import logging
import re
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException
from packaging.version import InvalidVersion, Version

from app.config import settings
from app.models.search_filters import SearchFilters

logger = logging.getLogger(__name__)

DEFAULT_CYCLE_DURATION_MS = settings.DEFAULT_CYCLE_DURATION_MS
MIN_INTERVAL_SECONDS = settings.MIN_INTERVAL_SECONDS


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


_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def parse_time(time_str: str) -> time:
    """Parse HH:MM string to time object with format and bounds validation.

    Args:
        time_str: Time in HH:MM 24-hour format (e.g., '06:30', '22:00').

    Returns:
        A time object representing the given time.

    Raises:
        HTTPException(422): If time_str is not valid HH:MM format or out of range.
    """
    match = _TIME_RE.match(time_str)
    if not match:
        raise HTTPException(status_code=422, detail="INVALID_TIME_FORMAT")
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        raise HTTPException(status_code=422, detail="INVALID_TIME_FORMAT")
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
