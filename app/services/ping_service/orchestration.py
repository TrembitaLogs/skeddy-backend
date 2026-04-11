"""Ping orchestration — high-level coordination extracted from the router.

Keeps the router focused on HTTP concerns (dependency injection, response
serialization) while this module owns the business flow decisions.
"""

import logging
from datetime import datetime
from uuid import UUID
from zoneinfo import ZoneInfo

from firebase_admin import exceptions as firebase_exceptions
from redis.asyncio import Redis
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.cluster_service import cluster_gate
from app.services.config_service.ping import PingConfigs
from app.services.credit_service import cache_balance
from app.services.fcm_service import send_ride_credit_refunded, send_search_update_required
from app.services.ping_service.schedule import calculate_dynamic_interval, is_within_schedule
from app.services.search_service import get_search_status

logger = logging.getLogger(__name__)


async def send_refund_notifications(
    db: AsyncSession,
    user_id: UUID,
    redis: Redis,
    cancelled_rides: list[dict],
) -> None:
    """Update Redis balance cache and send FCM pushes for cancelled rides.

    Called after a successful DB commit that contains refund transactions.
    FCM failures are logged but never propagated.
    """
    if not cancelled_rides:
        return

    last_balance = cancelled_rides[-1]["new_balance"]
    await cache_balance(user_id, last_balance, redis)

    for refund_info in cancelled_rides:
        try:
            await send_ride_credit_refunded(
                db,
                user_id,
                refund_info["ride_id"],
                refund_info["credits_refunded"],
                refund_info["new_balance"],
            )
        except (firebase_exceptions.FirebaseError, OperationalError):
            logger.warning(
                "FCM RIDE_CREDIT_REFUNDED failed in ping handler for ride %s",
                refund_info["ride_id"],
                exc_info=True,
            )


async def handle_force_update(
    db: AsyncSession,
    user_id: UUID,
    redis: Redis,
    min_search_version: str,
) -> None:
    """Send a one-per-hour FCM notification that the search app needs updating."""
    notified_key = f"search_update_notified:{user_id}"
    already_notified = await redis.get(notified_key)
    if not already_notified:
        await redis.setex(notified_key, 3600, "1")
        await send_search_update_required(db, user_id, min_search_version)


async def resolve_search_state(
    db: AsyncSession,
    user_id: UUID,
    redis: Redis,
    configs: PingConfigs,
    filters: object,
    timezone_str: str,
    last_cycle_duration_ms: int | None,
) -> tuple[bool, int]:
    """Determine whether the device should search and at what interval.

    Returns (search_active, interval_seconds).
    """
    search_status = await get_search_status(db, user_id)
    search_active = search_status.is_active and is_within_schedule(filters, timezone_str)

    if not search_active:
        return False, settings.PING_INTERVAL_INACTIVE

    # Calculate dynamic interval from configs
    if configs.search_interval_config is not None:
        rpd, rph = configs.search_interval_config
        tz = ZoneInfo(timezone_str)
        local_hour = datetime.now(tz).hour
        interval = calculate_dynamic_interval(rpd, rph, local_hour, last_cycle_duration_ms)
    else:
        interval = settings.DEFAULT_SEARCH_INTERVAL_SECONDS

    # Cluster gate — coordinate search among clustered devices
    cluster_result = await cluster_gate(
        device_id=str(user_id),
        redis=redis,
        clustering_enabled=configs.clustering_enabled,
    )
    if cluster_result is not None:
        search_active = cluster_result["search"]
        interval = cluster_result["interval_seconds"]

    return search_active, interval
