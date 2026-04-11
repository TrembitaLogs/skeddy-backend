import logging

from fastapi import APIRouter, Depends, Request, Response
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, limiter
from app.models.paired_device import PairedDevice
from app.redis import get_redis
from app.schemas.ping import PingFiltersResponse, PingRequest, PingResponse, VerifyRideItem
from app.services.config_service import batch_get_ping_configs
from app.services.credit_service import get_balance
from app.services.filter_service import get_user_filters
from app.services.ping_service import (
    build_verify_rides,
    check_app_version,
    handle_force_update,
    process_expired_verifications,
    process_ride_status_reports,
    process_stats_if_new,
    resolve_search_state,
    save_accept_failures,
    send_refund_notifications,
    update_device_state,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ping"])


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
    # 1. Timezone validated by PingRequest schema.

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
    await send_refund_notifications(db, device.user_id, redis, cancelled_rides)

    # Load filters — needed in ALL response paths per API contract.
    filters = await get_user_filters(db, device.user_id)
    filters_response = PingFiltersResponse(min_price=filters.min_price or 20.0)

    # 6. Batch-load all AppConfig values needed below (single Redis MGET).
    configs = await batch_get_ping_configs(db, redis)

    # 6a. Check app version.
    version_ok = check_app_version(body.app_version, configs.min_search_version)
    if not version_ok:
        device.last_interval_sent = settings.PING_INTERVAL_FORCE_UPDATE
        await db.commit()

        await handle_force_update(db, device.user_id, redis, configs.min_search_version)

        return PingResponse(
            search=False,
            force_update=True,
            update_url=settings.SEARCH_APP_UPDATE_URL,
            interval_seconds=settings.PING_INTERVAL_FORCE_UPDATE,
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
        device.last_interval_sent = settings.PING_INTERVAL_INACTIVE
        await db.commit()
        return PingResponse(
            search=False,
            reason="NO_CREDITS",
            interval_seconds=settings.PING_INTERVAL_INACTIVE,
            filters=filters_response,
            verify_rides=verify_rides,
        )

    # 9-10. Determine search state and calculate interval.
    search_active, interval = await resolve_search_state(
        db=db,
        user_id=device.user_id,
        device_id=device.id,
        redis=redis,
        configs=configs,
        filters=filters,
        timezone_str=body.timezone,
        last_cycle_duration_ms=body.last_cycle_duration_ms,
    )

    device.last_interval_sent = interval
    await db.commit()

    return PingResponse(
        search=search_active,
        interval_seconds=interval,
        filters=filters_response,
        verify_rides=verify_rides,
    )
