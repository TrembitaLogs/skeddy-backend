import logging
from uuid import UUID

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.redis import require_redis
from app.middleware.request_id import user_id_ctx
from app.models.user import User
from app.services.auth_service import decode_access_token, is_token_blacklisted

logger = logging.getLogger(__name__)

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(require_redis),
) -> User:
    """Extract and validate JWT from Authorization header, return the authenticated User."""
    payload = decode_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="INVALID_OR_EXPIRED_TOKEN")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="INVALID_TOKEN_PAYLOAD")

    # Check token blacklist (fail-closed: require_redis validates upfront,
    # but also catch mid-request Redis failures for resilience)
    jti = payload.get("jti")
    if jti:
        try:
            if await is_token_blacklisted(redis, jti):
                raise HTTPException(status_code=401, detail="TOKEN_REVOKED")
        except RedisError:
            logger.error("Redis unavailable during token blacklist check — denying access")
            raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    result = await db.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="USER_NOT_FOUND")

    # Reject tokens issued before the last password change
    if user.password_changed_at is not None:
        iat = payload.get("iat")
        if iat is not None:
            pw_changed_ts = user.password_changed_at.timestamp()
            if iat < pw_changed_ts:
                raise HTTPException(status_code=401, detail="TOKEN_REVOKED")

    # Inject user_id into logging context for all downstream log messages
    user_id_ctx.set(str(user.id))

    return user
