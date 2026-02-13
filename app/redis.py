import redis.asyncio as aioredis

from app.config import settings

redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)


async def get_redis():
    """FastAPI dependency that returns the async Redis client."""
    return redis_client
