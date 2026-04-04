import logging

from fastapi import Depends, HTTPException
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.redis import get_redis

logger = logging.getLogger(__name__)


async def require_redis(redis: Redis = Depends(get_redis)) -> Redis:
    """FastAPI dependency that verifies Redis is available before proceeding.

    Use this on endpoints that critically depend on Redis (e.g. verification
    codes, password reset tokens). Returns the Redis client on success,
    raises HTTP 503 if Redis is unreachable.
    """
    try:
        await redis.ping()  # type: ignore[misc]
    except RedisError:
        logger.error("Redis unavailable — endpoint requires Redis")
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")
    return redis
