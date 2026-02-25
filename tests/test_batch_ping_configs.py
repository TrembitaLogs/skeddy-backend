import json
from unittest.mock import AsyncMock

from redis.exceptions import RedisError

from app.config import settings
from app.models.app_config import AppConfig
from app.services.config_service import (
    CACHE_KEY,
    CACHE_KEY_INTERVAL,
    CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
    DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES,
    batch_get_ping_configs,
)

_WEIGHTS = [1.0] * 24


# ---------------------------------------------------------------------------
# Test 1: All values cached in Redis → single MGET, no DB query
# ---------------------------------------------------------------------------


async def test_all_cached_in_redis(db_session, fake_redis):
    """When all 3 keys are in Redis, MGET returns them and no DB query fires."""
    fake_redis._store[CACHE_KEY] = "2.0.0"
    fake_redis._store[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = "45"
    fake_redis._store[CACHE_KEY_INTERVAL] = json.dumps({"rpd": 1920, "rph": _WEIGHTS})

    result = await batch_get_ping_configs(db_session, fake_redis)

    assert result.min_search_version == "2.0.0"
    assert result.verification_check_interval_minutes == 45
    assert result.search_interval_config is not None
    rpd, rph = result.search_interval_config
    assert rpd == 1920
    assert rph == _WEIGHTS

    # Verify MGET was called exactly once
    fake_redis.mget.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test 2: Nothing cached → falls back to DB, caches results
# ---------------------------------------------------------------------------


async def test_all_from_db(db_session, fake_redis):
    """When Redis is empty, all values come from DB and get cached."""
    db_session.add(AppConfig(key="min_search_app_version", value="3.0.0"))
    db_session.add(AppConfig(key="verification_check_interval_minutes", value="90"))
    db_session.add(AppConfig(key="requests_per_day", value="2400"))
    db_session.add(AppConfig(key="requests_per_hour", value=json.dumps(_WEIGHTS)))
    await db_session.commit()

    result = await batch_get_ping_configs(db_session, fake_redis)

    assert result.min_search_version == "3.0.0"
    assert result.verification_check_interval_minutes == 90
    assert result.search_interval_config is not None
    rpd, rph = result.search_interval_config
    assert rpd == 2400
    assert rph == _WEIGHTS

    # Verify values were cached back in Redis
    assert fake_redis._store[CACHE_KEY] == "3.0.0"
    assert fake_redis._store[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] == "90"
    cached_interval = json.loads(fake_redis._store[CACHE_KEY_INTERVAL])
    assert cached_interval["rpd"] == 2400


# ---------------------------------------------------------------------------
# Test 3: Partial cache — some in Redis, rest from DB
# ---------------------------------------------------------------------------


async def test_partial_cache(db_session, fake_redis):
    """When only some keys are cached, missing ones are loaded from DB."""
    # Only min_version in Redis
    fake_redis._store[CACHE_KEY] = "1.5.0"

    # Rest in DB
    db_session.add(AppConfig(key="verification_check_interval_minutes", value="30"))
    db_session.add(AppConfig(key="requests_per_day", value="1000"))
    db_session.add(AppConfig(key="requests_per_hour", value=json.dumps(_WEIGHTS)))
    await db_session.commit()

    result = await batch_get_ping_configs(db_session, fake_redis)

    assert result.min_search_version == "1.5.0"
    assert result.verification_check_interval_minutes == 30
    assert result.search_interval_config is not None
    rpd, _ = result.search_interval_config
    assert rpd == 1000

    # min_version was NOT re-cached (already in Redis)
    # check_interval and interval were cached from DB
    assert fake_redis._store[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] == "30"
    assert CACHE_KEY_INTERVAL in fake_redis._store


# ---------------------------------------------------------------------------
# Test 4: Complete Redis failure → all from DB
# ---------------------------------------------------------------------------


async def test_redis_mget_failure_falls_back_to_db(db_session):
    """When Redis MGET raises RedisError, all values come from DB."""
    db_session.add(AppConfig(key="min_search_app_version", value="4.0.0"))
    db_session.add(AppConfig(key="verification_check_interval_minutes", value="15"))
    db_session.add(AppConfig(key="requests_per_day", value="500"))
    db_session.add(AppConfig(key="requests_per_hour", value=json.dumps(_WEIGHTS)))
    await db_session.commit()

    broken_redis = AsyncMock()
    broken_redis.mget = AsyncMock(side_effect=RedisError("connection refused"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("connection refused"))

    result = await batch_get_ping_configs(db_session, broken_redis)

    assert result.min_search_version == "4.0.0"
    assert result.verification_check_interval_minutes == 15
    assert result.search_interval_config is not None
    rpd, _ = result.search_interval_config
    assert rpd == 500


# ---------------------------------------------------------------------------
# Test 5: Neither Redis nor DB → defaults applied
# ---------------------------------------------------------------------------


async def test_defaults_when_nothing_configured(db_session, fake_redis):
    """When neither Redis nor DB has values, defaults are returned."""
    result = await batch_get_ping_configs(db_session, fake_redis)

    assert result.min_search_version == settings.MIN_SEARCH_APP_VERSION
    assert (
        result.verification_check_interval_minutes == DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES
    )
    assert result.search_interval_config is None


# ---------------------------------------------------------------------------
# Test 6: interval_config None when only one of rpd/rph exists in DB
# ---------------------------------------------------------------------------


async def test_interval_none_when_partial_db(db_session, fake_redis):
    """interval_config is None when only requests_per_day exists (no rph)."""
    db_session.add(AppConfig(key="requests_per_day", value="1920"))
    await db_session.commit()

    result = await batch_get_ping_configs(db_session, fake_redis)

    assert result.search_interval_config is None


# ---------------------------------------------------------------------------
# Test 7: Redis MGET failure + setex failure → still works from DB
# ---------------------------------------------------------------------------


async def test_redis_fully_broken(db_session):
    """Both MGET and setex fail → values from DB, no caching, no crash."""
    db_session.add(AppConfig(key="min_search_app_version", value="5.0.0"))
    db_session.add(AppConfig(key="verification_check_interval_minutes", value="120"))
    await db_session.commit()

    broken_redis = AsyncMock()
    broken_redis.mget = AsyncMock(side_effect=RedisError("down"))
    broken_redis.setex = AsyncMock(side_effect=RedisError("down"))

    result = await batch_get_ping_configs(db_session, broken_redis)

    assert result.min_search_version == "5.0.0"
    assert result.verification_check_interval_minutes == 120
    assert result.search_interval_config is None


# ---------------------------------------------------------------------------
# Test 8: Invalid cached values are ignored, fallback to DB
# ---------------------------------------------------------------------------


async def test_invalid_cached_values_fallback_to_db(db_session, fake_redis):
    """Corrupted Redis values are skipped and DB values are used instead."""
    fake_redis._store[CACHE_KEY_VERIFICATION_CHECK_INTERVAL] = "not_a_number"
    fake_redis._store[CACHE_KEY_INTERVAL] = "not valid json"

    db_session.add(AppConfig(key="verification_check_interval_minutes", value="20"))
    db_session.add(AppConfig(key="requests_per_day", value="800"))
    db_session.add(AppConfig(key="requests_per_hour", value=json.dumps(_WEIGHTS)))
    await db_session.commit()

    result = await batch_get_ping_configs(db_session, fake_redis)

    assert result.verification_check_interval_minutes == 20
    assert result.search_interval_config is not None
    rpd, _ = result.search_interval_config
    assert rpd == 800
