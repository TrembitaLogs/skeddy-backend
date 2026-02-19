from fastapi import APIRouter, Depends, HTTPException, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.redis import get_redis
from app.schemas.admin import VersionResponse, VersionUpdateRequest
from app.services.config_service import get_min_search_version, set_min_search_version

router = APIRouter(prefix="/api/admin/config", tags=["admin-config"])


async def require_admin(request: Request) -> None:
    """Dependency that checks for an authenticated admin session."""
    if not request.session.get("admin_authenticated"):
        raise HTTPException(status_code=401, detail="NOT_AUTHENTICATED")


@router.get(
    "/min-search-version",
    response_model=VersionResponse,
    dependencies=[Depends(require_admin)],
)
async def get_min_search_version_endpoint(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> VersionResponse:
    """Return the current minimum search app version."""
    version = await get_min_search_version(db, redis)
    return VersionResponse(min_search_app_version=version)


@router.put(
    "/min-search-version",
    response_model=VersionResponse,
    dependencies=[Depends(require_admin)],
)
async def put_min_search_version_endpoint(
    body: VersionUpdateRequest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> VersionResponse:
    """Update the minimum search app version."""
    await set_min_search_version(db, redis, body.version)
    return VersionResponse(min_search_app_version=body.version)
