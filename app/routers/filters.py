from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import get_current_user
from app.middleware.rate_limiter import get_user_key, limiter
from app.models.user import User
from app.schemas.auth import OkResponse
from app.schemas.filters import FiltersResponse, FiltersUpdateRequest
from app.services.filter_service import get_user_filters, update_user_filters

router = APIRouter(prefix="/filters", tags=["filters"])


@router.get("", response_model=FiltersResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def get_filters(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return current search filters for the authenticated user."""
    filters = await get_user_filters(db, current_user.id)
    return FiltersResponse(
        min_price=filters.min_price,
        start_time=filters.start_time,
        working_time=filters.working_time,
        working_days=list(filters.working_days),
    )


@router.put("", response_model=OkResponse)
@limiter.limit("60/minute", key_func=get_user_key)
async def update_filters(
    request: Request,
    response: Response,
    body: FiltersUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update search filters for the authenticated user."""
    await update_user_filters(db, current_user.id, body)
    return OkResponse()
