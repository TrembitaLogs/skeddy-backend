from unittest.mock import AsyncMock

from redis.exceptions import RedisError
from sqlalchemy import select

from app.config import settings
from app.models.app_config import AppConfig
from app.services.config_service import CACHE_KEY, get_min_search_version, set_min_search_version

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
