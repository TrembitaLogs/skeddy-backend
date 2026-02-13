import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.paired_device import PairedDevice
from app.models.search_filters import SearchFilters
from app.models.search_status import SearchStatus
from app.schemas.fcm import NotificationType, create_search_offline_payload
from app.services.fcm_service import send_push
from app.services.filter_service import get_user_filters
from app.services.ping_service import is_within_schedule

logger = logging.getLogger(__name__)


async def get_active_paired_devices(
    db,
) -> list[PairedDevice]:
    """Return all paired devices whose user has search enabled (is_active=True)."""
    stmt = (
        select(PairedDevice)
        .join(SearchStatus, PairedDevice.user_id == SearchStatus.user_id)
        .where(SearchStatus.is_active.is_(True))
        .options(joinedload(PairedDevice.user))
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


def should_notify_device_offline(
    device: PairedDevice,
    filters: SearchFilters,
    threshold_minutes: int,
    now_utc: datetime,
) -> bool:
    """Determine if an offline notification should be sent for a device.

    Returns True when ALL conditions are met:
    - Device has last_ping_at and timezone data
    - Current time is within the user's working schedule
    - Time since last ping exceeds the offline threshold
    - Device has not already been notified (offline_notified is False)

    Args:
        device: PairedDevice model instance.
        filters: User's SearchFilters for schedule checking.
        threshold_minutes: Minutes without ping to consider device offline.
        now_utc: Current UTC datetime (injected for testability).

    Returns:
        True if the device should receive an offline notification.
    """
    if device.last_ping_at is None or device.timezone is None:
        return False

    if not is_within_schedule(filters, device.timezone):
        return False

    elapsed = now_utc - device.last_ping_at
    if elapsed <= timedelta(minutes=threshold_minutes):
        return False

    return not device.offline_notified


def should_reset_offline_notified(
    device: PairedDevice,
    default_interval_seconds: int,
    now_utc: datetime,
) -> bool:
    """Determine if the offline_notified flag should be reset.

    Returns True when the device has come back online (recent ping
    within interval * 2) and the offline notification flag is still set.

    Args:
        device: PairedDevice model instance.
        default_interval_seconds: Fallback interval if device has no last_interval_sent.
        now_utc: Current UTC datetime.

    Returns:
        True if offline_notified should be reset to False.
    """
    if not device.offline_notified:
        return False

    if device.last_ping_at is None:
        return False

    interval = device.last_interval_sent or default_interval_seconds
    elapsed = now_utc - device.last_ping_at

    return elapsed < timedelta(seconds=interval * 2)


async def check_device_health() -> None:
    """Background task that checks device health every N minutes.

    Iterates over all paired devices with active search status.
    For each device:
    - Fetches user's search filters for schedule checking
    - Detects offline devices (elapsed > threshold + within schedule)
    - Sends FCM SEARCH_OFFLINE push when device goes offline
    - Detects recovery (device back online) and resets offline flag
    """
    interval = settings.HEALTH_CHECK_INTERVAL_MINUTES * 60
    logger.info(
        "Health check task started (interval=%d minutes)",
        settings.HEALTH_CHECK_INTERVAL_MINUTES,
    )

    while True:
        try:
            async with AsyncSessionLocal() as db:
                devices = await get_active_paired_devices(db)
                logger.debug("Health check: found %d active device(s)", len(devices))

                now_utc = datetime.now(UTC)

                for device in devices:
                    filters = await get_user_filters(db, device.user_id)

                    if should_notify_device_offline(
                        device,
                        filters,
                        settings.OFFLINE_NOTIFICATION_THRESHOLD_MINUTES,
                        now_utc,
                    ):
                        fcm_token = device.user.fcm_token if device.user else None
                        if fcm_token:
                            assert (
                                device.last_ping_at is not None
                            )  # guaranteed by should_notify_device_offline
                            payload = create_search_offline_payload(
                                device_id=device.device_id,
                                last_ping_at=device.last_ping_at,
                            )
                            sent = await send_push(
                                db,
                                fcm_token,
                                NotificationType.SEARCH_OFFLINE,
                                payload,
                                device.user_id,
                            )
                            if sent:
                                device.offline_notified = True
                                await db.commit()
                                logger.info(
                                    "Device %s offline notification sent (user=%s)",
                                    device.device_id,
                                    device.user_id,
                                )
                            else:
                                logger.warning(
                                    "Device %s offline push failed (user=%s), will retry",
                                    device.device_id,
                                    device.user_id,
                                )
                        else:
                            device.offline_notified = True
                            await db.commit()
                            logger.info(
                                "Device %s offline, no FCM token (user=%s), flagged",
                                device.device_id,
                                device.user_id,
                            )
                    elif should_reset_offline_notified(
                        device,
                        settings.DEFAULT_SEARCH_INTERVAL_SECONDS,
                        now_utc,
                    ):
                        device.offline_notified = False
                        await db.commit()
                        logger.info(
                            "Device %s back online (user=%s), reset offline_notified",
                            device.device_id,
                            device.user_id,
                        )
        except Exception:
            logger.exception("Health check error")

        await asyncio.sleep(interval)
