"""Tests for clustering config getters in config_service.clustering."""

from unittest.mock import AsyncMock

from redis.exceptions import RedisError

from app.models.app_config import AppConfig
from app.services.config_service import (
    CACHE_KEY_CLUSTERING_ENABLED,
    CACHE_KEY_CLUSTERING_PENALTY,
    CACHE_KEY_CLUSTERING_REBUILD_INTERVAL,
    CACHE_KEY_CLUSTERING_THRESHOLD,
    DEFAULT_CLUSTERING_ENABLED,
    DEFAULT_CLUSTERING_PENALTY_MINUTES,
    DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
    DEFAULT_CLUSTERING_THRESHOLD_MILES,
    get_clustering_enabled,
    get_clustering_penalty_minutes,
    get_clustering_rebuild_interval_minutes,
    get_clustering_threshold_miles,
)

# ===========================================================================
# get_clustering_enabled
# ===========================================================================


class TestGetClusteringEnabled:
    async def test_returns_cached_value_true(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_ENABLED] = "true"
        result = await get_clustering_enabled(db_session, fake_redis)
        assert result is True

    async def test_returns_cached_value_false(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_ENABLED] = "false"
        result = await get_clustering_enabled(db_session, fake_redis)
        assert result is False

    async def test_falls_back_to_db(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_enabled", value="true"))
        await db_session.commit()
        result = await get_clustering_enabled(db_session, fake_redis)
        assert result is True
        # Verify it was cached
        assert fake_redis._store.get(CACHE_KEY_CLUSTERING_ENABLED) == "true"

    async def test_falls_back_to_default(self, db_session, fake_redis):
        result = await get_clustering_enabled(db_session, fake_redis)
        assert result is DEFAULT_CLUSTERING_ENABLED

    async def test_redis_error_falls_back_to_db(self, db_session, fake_redis):
        fake_redis.get = AsyncMock(side_effect=RedisError("down"))
        db_session.add(AppConfig(key="clustering_enabled", value="true"))
        await db_session.commit()
        result = await get_clustering_enabled(db_session, fake_redis)
        assert result is True


# ===========================================================================
# get_clustering_penalty_minutes
# ===========================================================================


class TestGetClusteringPenaltyMinutes:
    async def test_returns_cached_value(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_PENALTY] = "90"
        result = await get_clustering_penalty_minutes(db_session, fake_redis)
        assert result == 90

    async def test_falls_back_to_db(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_penalty_minutes", value="45"))
        await db_session.commit()
        result = await get_clustering_penalty_minutes(db_session, fake_redis)
        assert result == 45
        assert fake_redis._store.get(CACHE_KEY_CLUSTERING_PENALTY) == "45"

    async def test_falls_back_to_default(self, db_session, fake_redis):
        result = await get_clustering_penalty_minutes(db_session, fake_redis)
        assert result == DEFAULT_CLUSTERING_PENALTY_MINUTES

    async def test_invalid_db_value(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_penalty_minutes", value="not-a-number"))
        await db_session.commit()
        result = await get_clustering_penalty_minutes(db_session, fake_redis)
        assert result == DEFAULT_CLUSTERING_PENALTY_MINUTES

    async def test_redis_error_falls_back_to_db(self, db_session, fake_redis):
        fake_redis.get = AsyncMock(side_effect=RedisError("down"))
        db_session.add(AppConfig(key="clustering_penalty_minutes", value="120"))
        await db_session.commit()
        result = await get_clustering_penalty_minutes(db_session, fake_redis)
        assert result == 120


# ===========================================================================
# get_clustering_threshold_miles
# ===========================================================================


class TestGetClusteringThresholdMiles:
    async def test_returns_cached_value(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_THRESHOLD] = "20"
        result = await get_clustering_threshold_miles(db_session, fake_redis)
        assert result == 20

    async def test_falls_back_to_db(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_threshold_miles", value="10"))
        await db_session.commit()
        result = await get_clustering_threshold_miles(db_session, fake_redis)
        assert result == 10
        assert fake_redis._store.get(CACHE_KEY_CLUSTERING_THRESHOLD) == "10"

    async def test_falls_back_to_default(self, db_session, fake_redis):
        result = await get_clustering_threshold_miles(db_session, fake_redis)
        assert result == DEFAULT_CLUSTERING_THRESHOLD_MILES

    async def test_invalid_db_value(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_threshold_miles", value="abc"))
        await db_session.commit()
        result = await get_clustering_threshold_miles(db_session, fake_redis)
        assert result == DEFAULT_CLUSTERING_THRESHOLD_MILES


# ===========================================================================
# get_clustering_rebuild_interval_minutes
# ===========================================================================


class TestGetClusteringRebuildIntervalMinutes:
    async def test_returns_cached_value(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_REBUILD_INTERVAL] = "10"
        result = await get_clustering_rebuild_interval_minutes(db_session, fake_redis)
        assert result == 10

    async def test_falls_back_to_db(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_rebuild_interval_minutes", value="3"))
        await db_session.commit()
        result = await get_clustering_rebuild_interval_minutes(db_session, fake_redis)
        assert result == 3
        assert fake_redis._store.get(CACHE_KEY_CLUSTERING_REBUILD_INTERVAL) == "3"

    async def test_falls_back_to_default(self, db_session, fake_redis):
        result = await get_clustering_rebuild_interval_minutes(db_session, fake_redis)
        assert result == DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES

    async def test_invalid_db_value(self, db_session, fake_redis):
        db_session.add(AppConfig(key="clustering_rebuild_interval_minutes", value="???"))
        await db_session.commit()
        result = await get_clustering_rebuild_interval_minutes(db_session, fake_redis)
        assert result == DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES
