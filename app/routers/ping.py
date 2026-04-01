import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request, Response
from firebase_admin import exceptions as firebase_exceptions
from redis.asyncio import Redis
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, limiter
from app.models.paired_device import PairedDevice
from app.redis import get_redis
from app.schemas.ping import PingFiltersResponse, PingRequest, PingResponse, VerifyRideItem
from app.services.config_service import batch_get_ping_configs
from app.services.credit_service import cache_balance, get_balance
from app.services.fcm_service import send_ride_credit_refunded, send_search_update_required
from app.services.filter_service import get_user_filters
from app.services.ping_service import (
    build_verify_rides,
    calculate_dynamic_interval,
    check_app_version,
    is_within_schedule,
    process_expired_verifications,
    process_ride_status_reports,
    process_stats_if_new,
    save_accept_failures,
    update_device_state,
    validate_timezone,
)
from app.services.search_service import get_search_status

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ping"])

# Interval constants (seconds) per API contract.
# Active search interval comes from settings; others are fixed.
INTERVAL_INACTIVE = 60
INTERVAL_FORCE_UPDATE = 300


@router.post("/ping", response_model=PingResponse)
@limiter.limit("12/minute", key_func=get_device_key)
async def ping(
    request: Request,
    response: Response,
    body: PingRequest,
    device: PairedDevice = Depends(verify_device),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> PingResponse:
    """Handle ping from Search App.

    Flow (per PRD sections 6-7):
    1. Validate timezone (422 if invalid)
    2. Update device state (last_ping_at, timezone, health)
    3. Process stats (batch dedup via Redis) and save failures
    4. Process ride verification status reports
    5. Process expired verifications (auto-confirm/cancel + atomic refund)
    6. Check app version (force_update if outdated)
    7. Build verify_rides list (throttled, sent regardless of search state)
    8. Check credit balance (NO_CREDITS if balance <= 0)
    9. Determine search state: is_active AND within schedule
    10. Calculate interval, save to device, return response
    """
    # 1. Validate timezone
    validate_timezone(body.timezone)

    # 2. Update device state (last_ping_at, timezone, health fields).
    # Done early so health monitoring always has fresh data, even if
    # the app version is outdated.
    await update_device_state(db, device, body)

    # 3. Process stats (Redis batch dedup) and save accept failures.
    was_processed, failures = await process_stats_if_new(redis, body.stats)
    if was_processed and failures:
        await save_accept_failures(db, device.user_id, failures)
        await db.commit()

    # 4. Process ride verification status reports.
    await process_ride_status_reports(db, device.user_id, body.ride_statuses)

    # 5. Process expired verifications (auto-confirm/cancel with atomic refund).
    cancelled_rides = await process_expired_verifications(db, device.user_id, redis)
    await db.commit()

    # Update Redis balance cache and send FCM pushes after successful commit.
    if cancelled_rides:
        last_balance = cancelled_rides[-1]["new_balance"]
        await cache_balance(device.user_id, last_balance, redis)
        for refund_info in cancelled_rides:
            try:
                await send_ride_credit_refunded(
                    db,
                    device.user_id,
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

    # Load filters — needed in ALL response paths per API contract.
    filters = await get_user_filters(db, device.user_id)
    filters_response = PingFiltersResponse(min_price=filters.min_price)

    # 6. Batch-load all AppConfig values needed below (single Redis MGET).
    configs = await batch_get_ping_configs(db, redis)

    # 6a. Check app version.
    version_ok = check_app_version(body.app_version, configs.min_search_version)
    if not version_ok:
        device.last_interval_sent = INTERVAL_FORCE_UPDATE
        await db.commit()

        # Notify main app once per hour that search app needs update.
        notified_key = f"search_update_notified:{device.user_id}"
        already_notified = await redis.get(notified_key)
        if not already_notified:
            await redis.setex(notified_key, 3600, "1")
            await send_search_update_required(db, device.user_id, configs.min_search_version)

        return PingResponse(
            search=False,
            force_update=True,
            update_url=settings.SEARCH_APP_UPDATE_URL,
            interval_seconds=INTERVAL_FORCE_UPDATE,
            filters=filters_response,
        )

    # 7. Build verify_rides list (sent regardless of search state per PRD).
    verify_ride_hashes = await build_verify_rides(
        db=db,
        user_id=device.user_id,
        check_interval_minutes=configs.verification_check_interval_minutes,
        cycle_duration_ms=body.last_cycle_duration_ms,
        last_interval_sent=device.last_interval_sent,
    )
    verify_rides = [VerifyRideItem(ride_hash=h) for h in verify_ride_hashes]

    # 8. Check credit balance (Redis-first, fallback to DB).
    # Balance check comes AFTER ride verification processing (steps 4-5, 7)
    # but BEFORE search state decision — per PRD section 7.
    balance = await get_balance(device.user_id, db, redis)
    if balance <= 0:
        device.last_interval_sent = INTERVAL_INACTIVE
        await db.commit()
        return PingResponse(
            search=False,
            reason="NO_CREDITS",
            interval_seconds=INTERVAL_INACTIVE,
            filters=filters_response,
            verify_rides=verify_rides,
        )

    # 9. Determine search state: is_active AND within schedule.
    search_status = await get_search_status(db, device.user_id)
    search_active = search_status.is_active and is_within_schedule(filters, body.timezone)

    # 10. Calculate interval, save to device, return response.
    if not search_active:
        interval = INTERVAL_INACTIVE
    else:
        if configs.search_interval_config is not None:
            rpd, rph = configs.search_interval_config
            tz = ZoneInfo(body.timezone)
            local_hour = datetime.now(tz).hour
            interval = calculate_dynamic_interval(
                rpd, rph, local_hour, body.last_cycle_duration_ms
            )
        else:
            interval = settings.DEFAULT_SEARCH_INTERVAL_SECONDS
    device.last_interval_sent = interval
    await db.commit()

    return PingResponse(
        search=search_active,
        interval_seconds=interval,
        filters=filters_response,
        verify_rides=verify_rides,
    )
