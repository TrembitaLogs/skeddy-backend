"""Verification config: deadline and check interval."""

import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.app_config import AppConfig
from app.services.config_service.cache import (
    CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
    CACHE_KEY_VERIFICATION_DEADLINE,
    CACHE_TTL,
    _memory_cache,
)

logger = logging.getLogger(__name__)

DEFAULT_VERIFICATION_DEADLINE_MINUTES = 30
DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES = 60


async def get_verification_deadline_minutes(db: AsyncSession, redis: Redis) -> int:
    """Return verification deadline minutes from AppConfig.

    The verification deadline is calculated as pickup_time minus N minutes.
    Resolution order: Redis cache -> DB -> DEFAULT_VERIFICATION_DEADLINE_MINUTES (30).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_VERIFICATION_DEADLINE)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_VERIFICATION_DEADLINE] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_VERIFICATION_DEADLINE,
        )
        mem_value = _memory_cache.get(CACHE_KEY_VERIFICATION_DEADLINE)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "verification_deadline_minutes")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            minutes = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid verification_deadline_minutes value: %r, using default %d",
                config_value,
                DEFAULT_VERIFICATION_DEADLINE_MINUTES,
            )
            return DEFAULT_VERIFICATION_DEADLINE_MINUTES

        _memory_cache[CACHE_KEY_VERIFICATION_DEADLINE] = minutes
        try:
            await redis.setex(CACHE_KEY_VERIFICATION_DEADLINE, CACHE_TTL, str(minutes))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_VERIFICATION_DEADLINE,
            )
        return minutes

    return DEFAULT_VERIFICATION_DEADLINE_MINUTES


async def get_verification_check_interval_minutes(db: AsyncSession, redis: Redis) -> int:
    """Return verification check interval minutes from AppConfig.

    Controls how often each ride is included in verify_rides for the Search App.
    Special value 0 means verification is requested only right before the deadline.

    Resolution order: Redis cache -> DB -> DEFAULT (60 minutes).
    """
    # 1. Try Redis cache
    try:
        cached = await redis.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL)
        if cached is not None:
            value = int(cached)
            _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = value
            return value
    except (RedisError, ValueError):
        logger.warning(
            "Redis read failed for %s, falling back to DB",
            CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
        )
        mem_value = _memory_cache.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL)
        if mem_value is not None:
            return int(mem_value)

    # 2. Try DB
    result = await db.execute(
        select(AppConfig.value).where(AppConfig.key == "verification_check_interval_minutes")
    )
    config_value = result.scalar_one_or_none()

    if config_value is not None:
        try:
            minutes = int(config_value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid verification_check_interval_minutes value: %r, using default %d",
                config_value,
                DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES,
            )
            return DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES

        _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = minutes
        try:
            await redis.setex(CACHE_KEY_VERIFICATION_CHECK_INTERVAL, CACHE_TTL, str(minutes))
        except RedisError:
            logger.warning(
                "Redis unavailable when caching %s",
                CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
            )
        return minutes

    return DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES
