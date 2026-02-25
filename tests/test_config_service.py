import json
from unittest.mock import AsyncMock

from redis.exceptions import RedisError
from sqlalchemy import select

from app.config import settings
from app.models.app_config import AppConfig
from app.services.config_service import (
    CACHE_KEY,
    CACHE_KEY_INTERVAL,
    CACHE_KEY_VERIFICATION_DEADLINE,
    DEFAULT_VERIFICATION_DEADLINE_MINUTES,
    get_min_search_version,
    get_search_interval_config,
    get_verification_deadline_minutes,
    set_min_search_version,
)

# ---------------------------------------------------------------------------
# Test 1: Redis has cached value -> returns cached, no DB query
# ---------------------------------------------------------------------------


async def test_get_returns_cached_value(db_session, fake_redis):
    """get_min_search_version returns cached value from Redis without DB hit."""
    fake_redis._store[CACHE_KEY] = "2.0.0"

    result = await get_min_search_version(db_session, fake_redis)

    assert result == "2.0.0"


# ---------------------------------------------------------------------------
# Test 2: Redis miss -> reads DB -> caches value
# ---------------------------------------------------------------------------


async def test_get_falls_back_to_db(db_session, fake_redis):
    """get_min_search_version falls back to DB on Redis miss and caches result."""
    config = AppConfig(key="min_search_app_version", value="3.0.0")
    db_session.add(config)
    await db_session.commit()

    result = await get_min_search_version(db_session, fake_redis)

    assert result == "3.0.0"
    # Verify it was cached in Redis
    assert fake_redis._store.get(CACHE_KEY) == "3.0.0"


# ---------------------------------------------------------------------------
# Test 3: Redis miss + DB empty -> returns settings default
# ---------------------------------------------------------------------------


async def test_get_falls_back_to_settings(db_session, fake_redis):
    """get_min_search_version returns settings fallback when no DB row exists."""
    result = await get_min_search_version(db_session, fake_redis)

    assert result == settings.MIN_SEARCH_APP_VERSION


# ---------------------------------------------------------------------------
# Test 4: set invalidates Redis cache
# ---------------------------------------------------------------------------


async def test_set_invalidates_cache(db_session, fake_redis):
    """set_min_search_version upserts DB and deletes Redis cache key."""
    fake_redis._store[CACHE_KEY] = "1.0.0"

    await set_min_search_version(db_session, fake_redis, "4.0.0")

    # Cache should be invalidated
    assert CACHE_KEY not in fake_redis._store

    # DB should have the new value
    result = await db_session.execute(
        select(AppConfig.value).where(AppConfig.key == "min_search_app_version")
    )
    assert result.scalar_one() == "4.0.0"


# ---------------------------------------------------------------------------
# Test 5: Redis raises RedisError -> graceful fallback to DB
# ---------------------------------------------------------------------------


async def test_get_graceful_redis_failure(db_session):
    """get_min_search_version falls back to DB when Redis raises RedisError."""
    config = AppConfig(key="min_search_app_version", value="5.0.0")
    db_session.add(config)
    await db_session.commit()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_min_search_version(db_session, broken_redis)

    assert result == "5.0.0"


# ---------------------------------------------------------------------------
# Test 6: set upserts (insert when row absent, update when present)
# ---------------------------------------------------------------------------


async def test_set_upserts_value(db_session, fake_redis):
    """set_min_search_version inserts a new row then updates it on second call."""
    # First call: insert
    await set_min_search_version(db_session, fake_redis, "1.0.0")
    result = await db_session.execute(
        select(AppConfig.value).where(AppConfig.key == "min_search_app_version")
    )
    assert result.scalar_one() == "1.0.0"

    # Second call: update
    await set_min_search_version(db_session, fake_redis, "2.0.0")
    result = await db_session.execute(
        select(AppConfig.value).where(AppConfig.key == "min_search_app_version")
    )
    assert result.scalar_one() == "2.0.0"


# ===========================================================================
# get_search_interval_config tests
# ===========================================================================

_WEIGHTS = [
    5.23,
    5.19,
    4.97,
    4.28,
    3.07,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    3.69,
    5.10,
    6.24,
    4.96,
    5.06,
    5.18,
    4.59,
    4.57,
    5.91,
    5.58,
    5.98,
    5.29,
    5.15,
    4.96,
]


async def _seed_interval_configs(db_session, rpd=1920, rph=None):
    """Seed requests_per_day and requests_per_hour in DB."""
    if rph is None:
        rph = _WEIGHTS
    db_session.add(AppConfig(key="requests_per_day", value=str(rpd)))
    db_session.add(AppConfig(key="requests_per_hour", value=json.dumps(rph)))
    await db_session.commit()


# ---------------------------------------------------------------------------
# Test 7: Redis has cached interval config -> returns parsed tuple
# ---------------------------------------------------------------------------


async def test_interval_config_returns_cached(db_session, fake_redis):
    """get_search_interval_config returns cached value from Redis."""
    cache_blob = json.dumps({"rpd": 1920, "rph": _WEIGHTS})
    fake_redis._store[CACHE_KEY_INTERVAL] = cache_blob

    result = await get_search_interval_config(db_session, fake_redis)

    assert result is not None
    rpd, rph = result
    assert rpd == 1920
    assert rph == _WEIGHTS


# ---------------------------------------------------------------------------
# Test 8: Redis miss -> reads DB -> caches and returns
# ---------------------------------------------------------------------------


async def test_interval_config_falls_back_to_db(db_session, fake_redis):
    """get_search_interval_config falls back to DB and caches result."""
    await _seed_interval_configs(db_session)

    result = await get_search_interval_config(db_session, fake_redis)

    assert result is not None
    rpd, rph = result
    assert rpd == 1920
    assert rph == _WEIGHTS
    # Verify cached in Redis
    assert CACHE_KEY_INTERVAL in fake_redis._store


# ---------------------------------------------------------------------------
# Test 9: No DB rows -> returns None
# ---------------------------------------------------------------------------


async def test_interval_config_returns_none_when_missing(db_session, fake_redis):
    """get_search_interval_config returns None when no config rows exist."""
    result = await get_search_interval_config(db_session, fake_redis)
    assert result is None


# ---------------------------------------------------------------------------
# Test 10: Only one key present -> returns None
# ---------------------------------------------------------------------------


async def test_interval_config_returns_none_when_partial(db_session, fake_redis):
    """get_search_interval_config returns None when only one config key exists."""
    db_session.add(AppConfig(key="requests_per_day", value="1920"))
    await db_session.commit()

    result = await get_search_interval_config(db_session, fake_redis)
    assert result is None


# ---------------------------------------------------------------------------
# Test 11: Redis error -> graceful fallback to DB
# ---------------------------------------------------------------------------


async def test_interval_config_graceful_redis_failure(db_session):
    """get_search_interval_config falls back to DB when Redis raises RedisError."""
    await _seed_interval_configs(db_session)

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_search_interval_config(db_session, broken_redis)

    assert result is not None
    rpd, rph = result
    assert rpd == 1920
    assert rph == _WEIGHTS


# ===========================================================================
# get_verification_deadline_minutes tests
# ===========================================================================


async def test_verification_deadline_returns_cached(db_session, fake_redis):
    """Returns cached value from Redis."""
    fake_redis._store[CACHE_KEY_VERIFICATION_DEADLINE] = "45"

    result = await get_verification_deadline_minutes(db_session, fake_redis)

    assert result == 45


async def test_verification_deadline_falls_back_to_db(db_session, fake_redis):
    """Falls back to DB on Redis miss and caches result."""
    db_session.add(AppConfig(key="verification_deadline_minutes", value="20"))
    await db_session.commit()

    result = await get_verification_deadline_minutes(db_session, fake_redis)

    assert result == 20
    assert fake_redis._store.get(CACHE_KEY_VERIFICATION_DEADLINE) == "20"


async def test_verification_deadline_falls_back_to_default(db_session, fake_redis):
    """Returns default (30) when no DB row exists."""
    result = await get_verification_deadline_minutes(db_session, fake_redis)

    assert result == DEFAULT_VERIFICATION_DEADLINE_MINUTES


async def test_verification_deadline_invalid_db_value(db_session, fake_redis):
    """Returns default when DB value is not a valid integer."""
    db_session.add(AppConfig(key="verification_deadline_minutes", value="not_a_number"))
    await db_session.commit()

    result = await get_verification_deadline_minutes(db_session, fake_redis)

    assert result == DEFAULT_VERIFICATION_DEADLINE_MINUTES


async def test_verification_deadline_graceful_redis_failure(db_session):
    """Falls back to DB when Redis raises RedisError."""
    db_session.add(AppConfig(key="verification_deadline_minutes", value="15"))
    await db_session.commit()

    broken_redis = AsyncMock()
    broken_redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await get_verification_deadline_minutes(db_session, broken_redis)

    assert result == 15
