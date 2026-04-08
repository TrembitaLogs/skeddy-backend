"""Clustering config: enabled flag, penalty, threshold, rebuild interval."""

import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.app_config import AppConfig
from app.services.config_service.cache import (
    CACHE_KEY_CLUSTERING_ENABLED,
    CACHE_KEY_CLUSTERING_PENALTY,
    CACHE_KEY_CLUSTERING_REBUILD_INTERVAL,
    CACHE_KEY_CLUSTERING_THRESHOLD,
    CACHE_TTL,
    _memory_cache,
)

logger = logging.getLogger(__name__)

DEFAULT_CLUSTERING_ENABLED = False
DEFAULT_CLUSTERING_PENALTY_MINUTES = 60
DEFAULT_CLUSTERING_THRESHOLD_MILES = 16
DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES = 5


async def get_clustering_enabled(db: AsyncSession, redis: Redis) -> bool:
    """Return whether clustering is enabled.

    Resolution order: Redis cache -> DB -> DEFAULT_CLUSTERING_ENABLED (False).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_CLUSTERING_ENABLED)
        if cached is not None:
            value = cached.lower() in ("true", "1", "yes")
            _memory_cache[CACHE_KEY_CLUSTERING_ENABLED] = value
            return value
    except RedisError:
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_CLUSTERING_ENABLED,
        )
        mem_value = _memory_cache.get(CACHE_KEY_CLUSTERING_ENABLED)
        if mem_value is not None:
            return bool(mem_value)

    # 2. Try DB
    result = await db.execute(select(AppConfig.value).where(AppConfig.key == "clustering_enabled"))
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        value = config_value.lower() in ("true", "1", "yes")
        _memory_cache[CACHE_KEY_CLUSTERING_ENABLED] = value
        try:
            await redis.setex(CACHE_KEY_CLUSTERING_ENABLED, CACHE_TTL, str(value).lower())
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_CLUSTERING_ENABLED,
            )
        return value

    return DEFAULT_CLUSTERING_ENABLED


async def get_clustering_penalty_minutes(db: AsyncSession, redis: Redis) -> int:
    """Return clustering penalty minutes from AppConfig.

    Resolution order: Redis cache -> DB -> DEFAULT_CLUSTERING_PENALTY_MINUTES (60).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_CLUSTERING_PENALTY)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_CLUSTERING_PENALTY] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_CLUSTERING_PENALTY,
        )
        mem_value = _memory_cache.get(CACHE_KEY_CLUSTERING_PENALTY)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "clustering_penalty_minutes")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            minutes = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid clustering_penalty_minutes value: %r, using default %d",
                config_value,
                DEFAULT_CLUSTERING_PENALTY_MINUTES,
            )
            return DEFAULT_CLUSTERING_PENALTY_MINUTES

        _memory_cache[CACHE_KEY_CLUSTERING_PENALTY] = minutes
        try:
            await redis.setex(CACHE_KEY_CLUSTERING_PENALTY, CACHE_TTL, str(minutes))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_CLUSTERING_PENALTY,
            )
        return minutes

    return DEFAULT_CLUSTERING_PENALTY_MINUTES


async def get_clustering_threshold_miles(db: AsyncSession, redis: Redis) -> int:
    """Return clustering distance threshold in miles from AppConfig.

    Resolution order: Redis cache -> DB -> DEFAULT_CLUSTERING_THRESHOLD_MILES (16).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_CLUSTERING_THRESHOLD)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_CLUSTERING_THRESHOLD] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_CLUSTERING_THRESHOLD,
        )
        mem_value = _memory_cache.get(CACHE_KEY_CLUSTERING_THRESHOLD)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "clustering_threshold_miles")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            miles = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid clustering_threshold_miles value: %r, using default %d",
                config_value,
                DEFAULT_CLUSTERING_THRESHOLD_MILES,
            )
            return DEFAULT_CLUSTERING_THRESHOLD_MILES

        _memory_cache[CACHE_KEY_CLUSTERING_THRESHOLD] = miles
        try:
            await redis.setex(CACHE_KEY_CLUSTERING_THRESHOLD, CACHE_TTL, str(miles))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_CLUSTERING_THRESHOLD,
            )
        return miles

    return DEFAULT_CLUSTERING_THRESHOLD_MILES


async def get_clustering_rebuild_interval_minutes(db: AsyncSession, redis: Redis) -> int:
    """Return clustering rebuild interval in minutes from AppConfig.

    Resolution order: Redis cache -> DB -> DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES (5).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_CLUSTERING_REBUILD_INTERVAL)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_CLUSTERING_REBUILD_INTERVAL] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_CLUSTERING_REBUILD_INTERVAL,
        )
        mem_value = _memory_cache.get(CACHE_KEY_CLUSTERING_REBUILD_INTERVAL)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "clustering_rebuild_interval_minutes")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            minutes = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid clustering_rebuild_interval_minutes value: %r, using default %d",
                config_value,
                DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
            )
            return DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES

        _memory_cache[CACHE_KEY_CLUSTERING_REBUILD_INTERVAL] = minutes
        try:
            await redis.setex(CACHE_KEY_CLUSTERING_REBUILD_INTERVAL, CACHE_TTL, str(minutes))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_CLUSTERING_REBUILD_INTERVAL,
            )
        return minutes

    return DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES
