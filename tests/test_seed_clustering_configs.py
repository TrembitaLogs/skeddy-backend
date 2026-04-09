"""Tests for the clustering AppConfig seed migration.

Verifies that:
1. Migration data matches service-layer defaults.
2. Seeded values are correctly read by clustering getters.
3. Cache invalidation works for clustering keys.
"""

import pytest

from app.models.app_config import AppConfig
from app.services.config_service import (
    CACHE_KEY_CLUSTERING_ENABLED,
    CACHE_KEY_CLUSTERING_PENALTY,
    CACHE_KEY_CLUSTERING_REBUILD_INTERVAL,
    CACHE_KEY_CLUSTERING_THRESHOLD,
    invalidate_config,
)
from app.services.config_service.clustering import (
    DEFAULT_CLUSTERING_ENABLED,
    DEFAULT_CLUSTERING_PENALTY_MINUTES,
    DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
    DEFAULT_CLUSTERING_THRESHOLD_MILES,
    get_clustering_enabled,
    get_clustering_penalty_minutes,
    get_clustering_rebuild_interval_minutes,
    get_clustering_threshold_miles,
)
from migrations.versions.a3b4c5d6e7f8_seed_clustering_app_configs import (
    CLUSTERING_CONFIGS,
)

# ===========================================================================
# Migration data consistency
# ===========================================================================


class TestClusteringMigrationData:
    """Migration seed values must match service-layer defaults."""

    def test_contains_all_expected_keys(self):
        expected = {
            "clustering_enabled",
            "clustering_penalty_minutes",
            "clustering_threshold_miles",
            "clustering_rebuild_interval_minutes",
        }
        assert set(CLUSTERING_CONFIGS.keys()) == expected

    def test_clustering_enabled_matches_default(self):
        assert CLUSTERING_CONFIGS["clustering_enabled"] == str(DEFAULT_CLUSTERING_ENABLED).lower()

    def test_clustering_penalty_matches_default(self):
        assert CLUSTERING_CONFIGS["clustering_penalty_minutes"] == str(
            DEFAULT_CLUSTERING_PENALTY_MINUTES
        )

    def test_clustering_threshold_matches_default(self):
        assert CLUSTERING_CONFIGS["clustering_threshold_miles"] == str(
            DEFAULT_CLUSTERING_THRESHOLD_MILES
        )

    def test_clustering_rebuild_interval_matches_default(self):
        assert CLUSTERING_CONFIGS["clustering_rebuild_interval_minutes"] == str(
            DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES
        )


# ===========================================================================
# Integration: seeded rows -> getters return correct values
# ===========================================================================


class TestClusteringConfigGettersAfterSeed:
    """After inserting seed values, clustering getters must return them."""

    @pytest.fixture(autouse=True)
    async def _seed_clustering(self, db_session):
        """Insert clustering config rows matching the migration seed."""
        for key, value in CLUSTERING_CONFIGS.items():
            db_session.add(AppConfig(key=key, value=value))
        await db_session.flush()

    async def test_get_clustering_enabled(self, db_session, fake_redis):
        result = await get_clustering_enabled(db_session, fake_redis)
        assert result is False

    async def test_get_clustering_penalty_minutes(self, db_session, fake_redis):
        result = await get_clustering_penalty_minutes(db_session, fake_redis)
        assert result == 60

    async def test_get_clustering_threshold_miles(self, db_session, fake_redis):
        result = await get_clustering_threshold_miles(db_session, fake_redis)
        assert result == 16

    async def test_get_clustering_rebuild_interval(self, db_session, fake_redis):
        result = await get_clustering_rebuild_interval_minutes(db_session, fake_redis)
        assert result == 5


# ===========================================================================
# Cache invalidation for clustering keys
# ===========================================================================


class TestClusteringCacheInvalidation:
    """Invalidating clustering config keys removes them from Redis."""

    async def test_invalidate_clustering_enabled(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_ENABLED] = "true"
        await invalidate_config("clustering_enabled", fake_redis)
        assert CACHE_KEY_CLUSTERING_ENABLED not in fake_redis._store

    async def test_invalidate_clustering_penalty(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_PENALTY] = "60"
        await invalidate_config("clustering_penalty_minutes", fake_redis)
        assert CACHE_KEY_CLUSTERING_PENALTY not in fake_redis._store

    async def test_invalidate_clustering_threshold(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_THRESHOLD] = "16"
        await invalidate_config("clustering_threshold_miles", fake_redis)
        assert CACHE_KEY_CLUSTERING_THRESHOLD not in fake_redis._store

    async def test_invalidate_clustering_rebuild_interval(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_CLUSTERING_REBUILD_INTERVAL] = "5"
        await invalidate_config("clustering_rebuild_interval_minutes", fake_redis)
        assert CACHE_KEY_CLUSTERING_REBUILD_INTERVAL not in fake_redis._store
