"""Ping service — split into submodules by responsibility.

Re-exports all public symbols for backward compatibility.
"""

from app.services.ping_service.device import update_device_state
from app.services.ping_service.orchestration import (
    handle_force_update,
    resolve_search_state,
    send_refund_notifications,
)
from app.services.ping_service.schedule import (
    MIN_INTERVAL_SECONDS,
    calculate_dynamic_interval,
    check_app_version,
    is_within_schedule,
    parse_time,
    validate_timezone,
)
from app.services.ping_service.stats import (
    BATCH_DEDUP_TTL,
    BATCH_KEY_PREFIX,
    is_batch_already_processed,
    mark_batch_as_processed,
    process_stats_if_new,
    save_accept_failures,
)
from app.services.ping_service.verification import (
    build_verify_rides,
    process_expired_verifications,
    process_ride_status_reports,
)

__all__ = [
    "BATCH_DEDUP_TTL",
    "BATCH_KEY_PREFIX",
    "MIN_INTERVAL_SECONDS",
    "build_verify_rides",
    "calculate_dynamic_interval",
    "check_app_version",
    "handle_force_update",
    "is_batch_already_processed",
    "is_within_schedule",
    "mark_batch_as_processed",
    "parse_time",
    "process_expired_verifications",
    "process_ride_status_reports",
    "process_stats_if_new",
    "resolve_search_state",
    "save_accept_failures",
    "send_refund_notifications",
    "update_device_state",
    "validate_timezone",
]
