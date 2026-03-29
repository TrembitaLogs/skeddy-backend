import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.user import User
from app.redis import get_redis
from app.schemas.profile import UpdateProfileRequest, UpdateProfileResponse
from app.services.credit_service import cache_balance
from app.services.legacy_credit_service import try_claim_legacy_credits

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/profile", tags=["profile"])


@router.patch("", response_model=UpdateProfileResponse)
@limiter.limit("30/minute", key_func=get_user_key)
async def update_profile(
    request: Request,
    response: Response,
    body: UpdateProfileRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Update driver profile fields (phone_number, license_number).

    Only fields present in the request body are updated.
    Send null to clear a field. Omit a field to leave it unchanged.

    After a successful update, if both phone_number and license_number
    are set, the system automatically checks for legacy credits to transfer.
    """
    fields_to_update = body.model_fields_set

    if not fields_to_update:
        return UpdateProfileResponse()

    if "phone_number" in fields_to_update:
        if body.phone_number is not None:
            existing = await db.execute(
                select(User).where(
                    User.phone_number == body.phone_number, User.id != current_user.id
                )
            )
            if existing.scalar_one_or_none():
                raise HTTPException(status_code=409, detail="PHONE_ALREADY_EXISTS")
        current_user.phone_number = body.phone_number

    if "license_number" in fields_to_update:
        if body.license_number is not None:
            existing = await db.execute(
                select(User).where(
                    User.license_number == body.license_number, User.id != current_user.id
                )
            )
            if existing.scalar_one_or_none():
                raise HTTPException(status_code=409, detail="LICENSE_ALREADY_EXISTS")
        current_user.license_number = body.license_number

    # Auto-check legacy credits (uses flush, not commit)
    claimed = await try_claim_legacy_credits(
        user_id=current_user.id,
        phone_number=current_user.phone_number,
        license_number=current_user.license_number,
        db=db,
        redis=redis,
    )

    # Single commit for profile update + legacy claim
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        err = str(exc)
        if "license_number" in err:
            detail = "LICENSE_ALREADY_EXISTS"
        elif "phone_number" in err:
            detail = "PHONE_ALREADY_EXISTS"
        else:
            detail = "CONFLICT"
        raise HTTPException(status_code=409, detail=detail)

    # Update Redis cache if credits were claimed
    if claimed:
        from app.models.credit_balance import CreditBalance

        result = await db.execute(
            select(CreditBalance.balance).where(CreditBalance.user_id == current_user.id)
        )
        balance = result.scalar_one_or_none()
        if balance is not None:
            await cache_balance(current_user.id, balance, redis)

    return UpdateProfileResponse(legacy_credits_claimed=claimed)
