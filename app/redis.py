import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.config import settings

redis_client: Redis = None  # type: ignore[assignment]  # set by init_redis() in lifespan


def init_redis() -> Redis:
    """Create the async Redis client. Called once during FastAPI lifespan startup."""
    global redis_client
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return redis_client


async def close_redis() -> None:
    """Gracefully close the Redis connection pool. Called during lifespan shutdown."""
    global redis_client
    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None  # type: ignore[assignment]


async def get_redis() -> Redis:
    """FastAPI dependency that returns the async Redis client."""
    assert redis_client is not None, "Redis not initialized — call init_redis() in lifespan"
    return redis_client
