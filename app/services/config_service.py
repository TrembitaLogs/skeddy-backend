import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.app_config import AppConfig

logger = logging.getLogger(__name__)

CACHE_KEY = "app_config:min_search_app_version"
CACHE_KEY_INTERVAL = "app_config:search_interval"
CACHE_TTL = 300  # 5 minutes


async def get_min_search_version(db: AsyncSession, redis: Redis) -> str:
    """Return the minimum search app version.

    Resolution order: Redis cache -> DB -> settings fallback.
    Redis failures are handled gracefully (falls back to DB).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY)
        if cached is not None:
            return str(cached)
    except RedisError:
        logger.warning("Redis unavailable when reading %s, falling back to DB", CACHE_KEY)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "min_search_app_version")
    )
    row = result.scalar_one_or_none()

    if row is not None:
        # Cache the value for next time
        try:
            await redis.setex(CACHE_KEY, CACHE_TTL, row)
        except RedisError:
            logger.warning("Redis unavailable when caching %s", CACHE_KEY)
        return row

    # 3. Fallback to settings
    return settings.MIN_SEARCH_APP_VERSION


async def set_min_search_version(db: AsyncSession, redis: Redis, version: str) -> None:
    """Upsert the minimum search app version and invalidate cache."""
    stmt = (
        insert(AppConfig)
        .values(key="min_search_app_version", value=version)
        .on_conflict_do_update(index_elements=["key"], set_={"value": version})
    )
    await db.execute(stmt)
    await db.commit()

    try:
        await redis.delete(CACHE_KEY)
    except RedisError:
        logger.warning("Redis unavailable when invalidating %s", CACHE_KEY)


async def get_search_interval_config(
    db: AsyncSession, redis: Redis
) -> tuple[int, list[float]] | None:
    """Return (requests_per_day, requests_per_hour) for dynamic interval calculation.

    Resolution order: Redis cache -> DB -> None (caller uses flat default).
    Both values are cached together as a single JSON blob to avoid two DB queries.
    Redis failures are handled gracefully (falls back to DB).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_INTERVAL)
        if cached is not None:
            data = json.loads(cached)
            return (data["rpd"], data["rph"])
    except RedisError:
        logger.warning(
            "Redis unavailable when reading %s, falling back to DB",
            CACHE_KEY_INTERVAL,
        )

    # 2. Try DB — fetch both keys in one query
    result = await db.execute(
        select(AppConfig.key, AppConfig.value).where(
            AppConfig.key.in_(["requests_per_day", "requests_per_hour"])
        )
    )
    rows = {row.key: row.value for row in result.all()}

    if "requests_per_day" not in rows or "requests_per_hour" not in rows:
        return None

    rpd = int(rows["requests_per_day"])
    rph = json.loads(rows["requests_per_hour"])

    # Cache for next time
    cache_blob = json.dumps({"rpd": rpd, "rph": rph})
    try:
        await redis.setex(CACHE_KEY_INTERVAL, CACHE_TTL, cache_blob)
    except RedisError:
        logger.warning("Redis unavailable when caching %s", CACHE_KEY_INTERVAL)

    return (rpd, rph)
