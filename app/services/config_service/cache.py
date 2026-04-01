"""Cache infrastructure: keys, TTL, in-memory fallback, invalidation."""

import logging
from typing import Any

from cachetools import TTLCache
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache keys & TTL
# ---------------------------------------------------------------------------

CACHE_KEY = "app_config:min_search_app_version"
CACHE_KEY_INTERVAL = "app_config:search_interval"
CACHE_KEY_VERIFICATION_DEADLINE = "app_config:verification_deadline"
CACHE_KEY_VERIFICATION_CHECK_INTERVAL = "app_config:verification_check_interval"
CACHE_KEY_CREDIT_PRODUCTS = "app_config:credit_products"
CACHE_KEY_RIDE_CREDIT_TIERS = "app_config:ride_credit_tiers"
CACHE_KEY_REGISTRATION_BONUS = "app_config:registration_bonus"
CACHE_KEY_PUSH_TEMPLATES = "app_config:push_templates"
CACHE_KEY_EMAIL_TEMPLATES = "app_config:email_templates"
CACHE_TTL = 300  # 5 minutes (Redis)
IN_MEMORY_TTL = 600  # 10 minutes (fallback when Redis is unavailable)

# ---------------------------------------------------------------------------
# In-memory fallback cache (safety net when Redis is unavailable)
# ---------------------------------------------------------------------------

_memory_cache: TTLCache[str, Any] = TTLCache(maxsize=64, ttl=IN_MEMORY_TTL)

# Mapping: AppConfig DB key -> Redis cache key(s) to invalidate.
_DB_KEY_TO_CACHE_KEYS: dict[str, list[str]] = {
    "min_search_app_version": [CACHE_KEY],
    "requests_per_day": [CACHE_KEY_INTERVAL],
    "requests_per_hour": [CACHE_KEY_INTERVAL],
    "verification_deadline_minutes": [CACHE_KEY_VERIFICATION_DEADLINE],
    "verification_check_interval_minutes": [CACHE_KEY_VERIFICATION_CHECK_INTERVAL],
    "credit_products": [CACHE_KEY_CREDIT_PRODUCTS],
    "ride_credit_tiers": [CACHE_KEY_RIDE_CREDIT_TIERS],
    "registration_bonus_credits": [CACHE_KEY_REGISTRATION_BONUS],
    "push_notification_templates": [CACHE_KEY_PUSH_TEMPLATES],
}


async def invalidate_config(db_key: str, redis: Redis) -> None:
    """Remove a config value from both Redis and in-memory cache.

    Called from SQLAdmin ``after_model_change`` when an AppConfig entry is
    created or updated.  ``db_key`` is the ``AppConfig.key`` column value
    (e.g. ``"credit_products"``).
    """
    cache_keys = _DB_KEY_TO_CACHE_KEYS.get(db_key, [])
    for cache_key in cache_keys:
        _memory_cache.pop(cache_key, None)
        try:
            await redis.delete(cache_key)
        except RedisError:
            logger.warning("Redis unavailable when invalidating %s", cache_key)
