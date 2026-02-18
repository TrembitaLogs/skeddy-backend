from fastapi import APIRouter, Depends, Request, Response
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.user import User
from app.redis import get_redis
from app.schemas.auth import OkResponse
from app.schemas.pairing import (
    ConfirmPairingRequest,
    ConfirmPairingResponse,
    GeneratePairingResponse,
    PairingStatusResponse,
)
from app.services.pairing_service import (
    confirm_pairing,
    delete_accept_failures,
    delete_paired_device,
    generate_pairing_code,
    get_device_by_user_id,
)
from app.services.search_service import set_search_active

router = APIRouter(prefix="/pairing", tags=["pairing"])


@router.post("/generate", response_model=GeneratePairingResponse, status_code=201)
@limiter.limit("60/minute", key_func=get_user_key)
async def generate_code(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    redis: Redis = Depends(get_redis),
):
    """Generate 6-digit pairing code for device linking (requires user auth)."""
    code, expires_at = await generate_pairing_code(redis, current_user.id)
    return GeneratePairingResponse(code=code, expires_at=expires_at)


@router.post("/confirm", response_model=ConfirmPairingResponse)
@limiter.limit("5/minute")
async def confirm_code(
    request: Request,
    response: Response,
    body: ConfirmPairingRequest,
    redis: Redis = Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    """Confirm pairing code from Search App (no auth required, code is auth)."""
    device_token, user_id = await confirm_pairing(
        code=body.code,
        device_id=body.device_id,
        timezone_str=body.timezone,
        redis=redis,
        db=db,
        device_model=body.device_model,
    )
    return ConfirmPairingResponse(device_token=device_token, user_id=user_id)


@router.get("/status", response_model=PairingStatusResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def pairing_status(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Check current pairing status (requires user auth)."""
    device = await get_device_by_user_id(db, current_user.id)
    if device:
        return PairingStatusResponse(
            paired=True,
            device_id=device.device_id,
            device_model=device.device_model,
        )
    return PairingStatusResponse(paired=False)


@router.delete("", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def unpair_device(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Unpair search device. Idempotent — returns 200 even if no device paired."""
    device = await get_device_by_user_id(db, current_user.id)
    if device:
        await delete_paired_device(db, device.id)
        await delete_accept_failures(db, current_user.id)
        await set_search_active(db, current_user.id, active=False)
    await db.commit()
    return OkResponse()
