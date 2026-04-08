"""Batch loading for ping handler."""

import contextlib
import json
import logging
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.app_config import AppConfig
from app.services.config_service.cache import (
    CACHE_KEY,
    CACHE_KEY_CLUSTERING_ENABLED,
    CACHE_KEY_INTERVAL,
    CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
    CACHE_TTL,
    _memory_cache,
)
from app.services.config_service.clustering import DEFAULT_CLUSTERING_ENABLED
from app.services.config_service.verification import (
    DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PingConfigs:
    """All AppConfig values needed by the ping handler."""

    min_search_version: str
    verification_check_interval_minutes: int
    search_interval_config: tuple[int, list[float]] | None
    clustering_enabled: bool


# DB keys that map to each Redis cache entry.
_DB_KEY_MIN_VERSION = "min_search_app_version"
_DB_KEY_CHECK_INTERVAL = "verification_check_interval_minutes"
_DB_KEY_RPD = "requests_per_day"
_DB_KEY_RPH = "requests_per_hour"
_DB_KEY_CLUSTERING_ENABLED = "clustering_enabled"


async def batch_get_ping_configs(db: AsyncSession, redis: Redis) -> PingConfigs:
    """Load all AppConfig values needed by ping in a single Redis MGET.

    Falls back to a single DB query (IN clause) for any keys missing from
    the Redis cache, then writes them back for future requests.

    Reduces Redis round-trips from 3 separate GETs to 1 MGET.
    """
    redis_keys = [
        CACHE_KEY,
        CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
        CACHE_KEY_INTERVAL,
        CACHE_KEY_CLUSTERING_ENABLED,
    ]

    # 1. Try Redis MGET
    cached: list[str | None] = [None, None, None, None]
    redis_failed = False
    try:
        cached = await redis.mget(redis_keys)
    except RedisError:
        redis_failed = True
        logger.warning("Redis MGET failed for ping configs, falling back to DB")

    # 2. Parse cached values
    min_version: str | None = None
    check_interval: int | None = None
    interval_config: tuple[int, list[float]] | None = None
    clustering_enabled: bool | None = None
    # Track whether interval was resolved (None is a valid "not configured" state)
    interval_resolved = False

    if cached[0] is not None:
        min_version = str(cached[0])
        _memory_cache[CACHE_KEY] = min_version

    if cached[1] is not None:
        try:
            check_interval = int(cached[1])
            _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = check_interval
        except (ValueError, TypeError):
            logger.warning("Invalid cached verification_check_interval: %r", cached[1])

    if cached[2] is not None:
        try:
            data = json.loads(cached[2])
            interval_config = (data["rpd"], data["rph"])
            interval_resolved = True
            _memory_cache[CACHE_KEY_INTERVAL] = interval_config
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Invalid cached search_interval: %r", cached[2])

    if cached[3] is not None:
        clustering_enabled = str(cached[3]).lower() in ("true", "1", "yes")
        _memory_cache[CACHE_KEY_CLUSTERING_ENABLED] = clustering_enabled

    # 2b. In-memory fallback for values still missing after Redis failure
    if redis_failed:
        if min_version is None:
            mem = _memory_cache.get(CACHE_KEY)
            if mem is not None:
                min_version = mem
        if check_interval is None:
            mem = _memory_cache.get(CACHE_KEY_VERIFICATION_CHECK_INTERVAL)
            if mem is not None:
                check_interval = mem
        if not interval_resolved:
            mem = _memory_cache.get(CACHE_KEY_INTERVAL)
            if mem is not None:
                interval_config = mem
                interval_resolved = True
        if clustering_enabled is None:
            mem = _memory_cache.get(CACHE_KEY_CLUSTERING_ENABLED)
            if mem is not None:
                clustering_enabled = mem

    # 3. Determine which DB keys are needed
    need_db_keys: list[str] = []
    if min_version is None:
        need_db_keys.append(_DB_KEY_MIN_VERSION)
    if check_interval is None:
        need_db_keys.append(_DB_KEY_CHECK_INTERVAL)
    if not interval_resolved:
        need_db_keys.extend([_DB_KEY_RPD, _DB_KEY_RPH])
    if clustering_enabled is None:
        need_db_keys.append(_DB_KEY_CLUSTERING_ENABLED)

    # 4. Single DB query for all missing keys
    if need_db_keys:
        result = await db.execute(
            select(AppConfig.key, AppConfig.value).where(AppConfig.key.in_(need_db_keys))
        )
        db_rows = {row.key: row.value for row in result.all()}

        # Parse and cache min_version
        if min_version is None:
            db_val = db_rows.get(_DB_KEY_MIN_VERSION)
            if db_val is not None:
                min_version = db_val
                _memory_cache[CACHE_KEY] = min_version
                with contextlib.suppress(RedisError):
                    await redis.setex(CACHE_KEY, CACHE_TTL, db_val)

        # Parse and cache check_interval
        if check_interval is None:
            db_val = db_rows.get(_DB_KEY_CHECK_INTERVAL)
            if db_val is not None:
                try:
                    check_interval = int(db_val)
                    _memory_cache[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = check_interval
                    with contextlib.suppress(RedisError):
                        await redis.setex(
                            CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
                            CACHE_TTL,
                            db_val,
                        )
                except (ValueError, TypeError):
                    logger.warning("Invalid DB verification_check_interval_minutes: %r", db_val)

        # Parse and cache interval_config
        if not interval_resolved:
            rpd_val = db_rows.get(_DB_KEY_RPD)
            rph_val = db_rows.get(_DB_KEY_RPH)
            if rpd_val is not None and rph_val is not None:
                try:
                    rpd = int(rpd_val)
                    rph = json.loads(rph_val)
                    interval_config = (rpd, rph)
                    interval_resolved = True
                    _memory_cache[CACHE_KEY_INTERVAL] = interval_config
                    cache_blob = json.dumps({"rpd": rpd, "rph": rph})
                    with contextlib.suppress(RedisError):
                        await redis.setex(CACHE_KEY_INTERVAL, CACHE_TTL, cache_blob)
                except (ValueError, TypeError, json.JSONDecodeError):
                    logger.warning("Invalid DB interval config")

        # Parse and cache clustering_enabled
        if clustering_enabled is None:
            db_val = db_rows.get(_DB_KEY_CLUSTERING_ENABLED)
            if db_val is not None:
                clustering_enabled = db_val.lower() in ("true", "1", "yes")
                _memory_cache[CACHE_KEY_CLUSTERING_ENABLED] = clustering_enabled
                with contextlib.suppress(RedisError):
                    await redis.setex(
                        CACHE_KEY_CLUSTERING_ENABLED,
                        CACHE_TTL,
                        str(clustering_enabled).lower(),
                    )

    # 5. Apply defaults for anything still missing
    if min_version is None:
        min_version = settings.MIN_SEARCH_APP_VERSION
    if check_interval is None:
        check_interval = DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES
    if clustering_enabled is None:
        clustering_enabled = DEFAULT_CLUSTERING_ENABLED
    # interval_config stays None when not configured (caller uses flat default)

    return PingConfigs(
        min_search_version=min_version,
        verification_check_interval_minutes=check_interval,
        search_interval_config=interval_config,
        clustering_enabled=clustering_enabled,
    )
