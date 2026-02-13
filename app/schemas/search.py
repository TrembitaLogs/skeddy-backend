from datetime import UTC, datetime

from pydantic import BaseModel


def calculate_is_online(last_ping_at: datetime | None, interval: int) -> bool:
    """Check if device is online based on last ping time.

    A device is considered online if ``last_ping_at`` is within
    ``interval * 2`` seconds from now (UTC).  The 2x multiplier gives
    a grace period for network jitter and processing delays.
    """
    if last_ping_at is None:
        return False
    elapsed = (datetime.now(UTC) - last_ping_at).total_seconds()
    return elapsed < interval * 2


class SearchStatusResponse(BaseModel):
    """Response schema for GET /search/status."""

    is_active: bool
    is_online: bool
    last_ping_at: datetime | None


class DeviceOverrideRequest(BaseModel):
    """Request schema for POST /search/device-override."""

    active: bool
