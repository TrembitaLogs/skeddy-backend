from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.dependencies.auth import get_current_user
from app.dependencies.device_auth import verify_device
from app.middleware.rate_limiter import get_device_key, get_user_key, limiter
from app.models.paired_device import PairedDevice
from app.models.user import User
from app.schemas.auth import OkResponse
from app.schemas.search import (
    DeviceOverrideRequest,
    SearchStatusResponse,
    calculate_is_online,
)
from app.services.pairing_service import get_device_by_user_id
from app.services.search_service import get_search_status, set_search_active

router = APIRouter(prefix="/search", tags=["search"])


@router.post("/start", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def start_search(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Start search for the authenticated user. Requires a paired device."""
    device = await get_device_by_user_id(db, current_user.id)
    if device is None:
        raise HTTPException(status_code=400, detail="NO_PAIRED_DEVICE")
    await set_search_active(db, current_user.id, active=True)
    return OkResponse()


@router.post("/stop", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def stop_search(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Stop search for the authenticated user. Idempotent."""
    await set_search_active(db, current_user.id, active=False)
    return OkResponse()


@router.get("/status", response_model=SearchStatusResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def get_status(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return search status with device online information."""
    status = await get_search_status(db, current_user.id)
    device = await get_device_by_user_id(db, current_user.id)

    is_online = False
    last_ping_at = None
    if device is not None:
        interval = device.last_interval_sent or settings.DEFAULT_SEARCH_INTERVAL_SECONDS
        is_online = calculate_is_online(device.last_ping_at, interval)
        last_ping_at = device.last_ping_at

    return SearchStatusResponse(
        is_active=status.is_active,
        is_online=is_online,
        last_ping_at=last_ping_at,
    )


@router.post("/device-override", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_device_key)
async def device_override(
    request: Request,
    response: Response,
    body: DeviceOverrideRequest,
    device: PairedDevice = Depends(verify_device),
    db: AsyncSession = Depends(get_db),
):
    """Override search active status from a paired Search device."""
    await set_search_active(db, device.user_id, active=body.active)
    return OkResponse()
