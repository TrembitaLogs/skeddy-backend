"""Ride service — split into submodules by responsibility.

Re-exports all public symbols for backward compatibility.
"""

from app.services.ride_service.billing import (
    create_ride_with_charge,
    send_ride_notifications,
)
from app.services.ride_service.crud import (
    create_ride,
    get_ride_by_idempotency,
    get_user_fcm_token,
    get_user_ride_events,
    resolve_ride_timezone,
)
from app.services.ride_service.events import (
    _EVENTS_CUTOFF_WEEKS,
    get_unified_events,
)
from app.services.ride_service.pickup_time import (
    _DAY_ABBREVS,
    _MONTH_ABBREVS,
    _MONTH_DAY_RE,
    _SEPARATORS,
    _TIME_RE,
    _parse_time_part,
    _resolve_date,
    calculate_verification_deadline,
    parse_pickup_time,
)

__all__ = [
    "_DAY_ABBREVS",
    "_EVENTS_CUTOFF_WEEKS",
    "_MONTH_ABBREVS",
    "_MONTH_DAY_RE",
    "_SEPARATORS",
    "_TIME_RE",
    "_parse_time_part",
    "_resolve_date",
    "calculate_verification_deadline",
    "create_ride",
    "create_ride_with_charge",
    "get_ride_by_idempotency",
    "get_unified_events",
    "get_user_fcm_token",
    "get_user_ride_events",
    "parse_pickup_time",
    "resolve_ride_timezone",
    "send_ride_notifications",
]
