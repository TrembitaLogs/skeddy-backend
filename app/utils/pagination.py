"""Cursor-based pagination utilities for the unified event feed."""

from datetime import UTC, datetime
from uuid import UUID

VALID_EVENT_KINDS = frozenset({"ride", "credit"})


def encode_cursor(created_at: datetime, event_kind: str, event_id: UUID) -> str:
    """Encode pagination cursor from components.

    Format: {iso_datetime_utc}_{event_kind}_{uuid}
    Example: 2026-02-21T10:30:00Z_ride_550e8400-e29b-41d4-a716-446655440000

    Args:
        created_at: Event timestamp (must be timezone-aware).
        event_kind: Event type - "ride" or "credit".
        event_id: UUID of the event.

    Returns:
        Encoded cursor string.

    Raises:
        ValueError: If event_kind is not valid or datetime is naive.
    """
    if event_kind not in VALID_EVENT_KINDS:
        raise ValueError(
            f"Invalid event_kind '{event_kind}', must be one of {sorted(VALID_EVENT_KINDS)}"
        )

    if created_at.tzinfo is None:
        raise ValueError("created_at must be timezone-aware")

    utc_dt = created_at.astimezone(UTC)

    if utc_dt.microsecond:
        iso_str = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    else:
        iso_str = utc_dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"

    return f"{iso_str}_{event_kind}_{event_id}"


def decode_cursor(cursor: str) -> tuple[datetime, str, UUID]:
    """Decode pagination cursor into components.

    Args:
        cursor: Encoded cursor string.

    Returns:
        Tuple of (created_at as UTC datetime, event_kind, event UUID).

    Raises:
        ValueError: If cursor format is invalid.
    """
    parts = cursor.split("_", 2)
    if len(parts) != 3:
        raise ValueError(
            f"Invalid cursor format: expected 3 parts separated by '_', got {len(parts)}"
        )

    iso_str, event_kind, uuid_str = parts

    if event_kind not in VALID_EVENT_KINDS:
        raise ValueError(
            f"Invalid event_kind '{event_kind}' in cursor,"
            f" must be one of {sorted(VALID_EVENT_KINDS)}"
        )

    if not iso_str.endswith("Z"):
        raise ValueError(f"Invalid datetime in cursor: must end with 'Z' (UTC), got '{iso_str}'")

    try:
        iso_clean = iso_str[:-1]
        if "." in iso_clean:
            dt = datetime.strptime(iso_clean, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            dt = datetime.strptime(iso_clean, "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=UTC)
    except ValueError as exc:
        raise ValueError(f"Invalid datetime in cursor: '{iso_str}'") from exc

    try:
        event_id = UUID(uuid_str)
    except ValueError as exc:
        raise ValueError(f"Invalid UUID in cursor: '{uuid_str}'") from exc

    return dt, event_kind, event_id
