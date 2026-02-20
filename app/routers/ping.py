import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request, Response
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, limiter
from app.models.paired_device import PairedDevice
from app.redis import get_redis
from app.schemas.ping import PingFiltersResponse, PingRequest, PingResponse
from app.services.config_service import get_min_search_version, get_search_interval_config
from app.services.filter_service import get_user_filters
from app.services.ping_service import (
    calculate_dynamic_interval,
    check_app_version,
    is_within_schedule,
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

    Flow (per PRD section 6):
    1. Validate timezone (422 if invalid)
    2. Update device state (last_ping_at, timezone, health)
    3. Process stats (batch dedup via Redis) and save failures
    4. Check app version (force_update if outdated)
    5. Check is_active and schedule (DST-safe)
    6. Save interval to device and return response
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

    # Load filters — needed in ALL response paths per API contract.
    filters = await get_user_filters(db, device.user_id)
    filters_response = PingFiltersResponse(min_price=filters.min_price)

    # 4. Check app version.
    min_version = await get_min_search_version(db, redis)
    version_ok = check_app_version(body.app_version, min_version)
    if not version_ok:
        device.last_interval_sent = INTERVAL_FORCE_UPDATE
        await db.commit()
        return PingResponse(
            search=False,
            force_update=True,
            update_url=settings.SEARCH_APP_UPDATE_URL,
            interval_seconds=INTERVAL_FORCE_UPDATE,
            filters=filters_response,
        )

    # 5. Determine search state: is_active AND within schedule.
    search_status = await get_search_status(db, device.user_id)
    search_active = search_status.is_active and is_within_schedule(filters, body.timezone)

    # 6. Calculate interval, save to device, return response.
    if not search_active:
        interval = INTERVAL_INACTIVE
    else:
        interval_config = await get_search_interval_config(db, redis)
        if interval_config is not None:
            rpd, rph = interval_config
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
    )
