from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.paired_device import PairedDevice
from app.schemas.ping import PingRequest


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
