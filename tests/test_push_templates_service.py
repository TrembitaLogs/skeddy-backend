"""Tests for push templates config service (DB table getter + caching)."""

import json

import pytest_asyncio

from app.models.push_template import PushTemplate
from app.services.config_service import (
    CACHE_KEY_PUSH_TEMPLATES,
    DEFAULT_PUSH_TEMPLATES,
    get_push_templates,
    invalidate_push_templates,
)


class TestGetPushTemplates:
    """Tests for get_push_templates() with DB, Redis, and fallback."""

    @pytest_asyncio.fixture
    async def seed_templates(self, db_session):
        """Seed push_templates table with test data."""
        for ntype, langs in DEFAULT_PUSH_TEMPLATES.items():
            row = PushTemplate(
                notification_type=ntype,
                title_en=langs["en"]["title"],
                body_en=langs["en"]["body"],
                title_es=f"{ntype} ES title",
                body_es=f"{ntype} ES body",
            )
            db_session.add(row)
        await db_session.flush()

    async def test_returns_defaults_when_table_empty(self, db_session, fake_redis):
        result = await get_push_templates(db_session, fake_redis)
        assert "RIDE_ACCEPTED" in result.root
        assert "CREDITS_DEPLETED" in result.root
        assert len(result.root) == len(DEFAULT_PUSH_TEMPLATES)

    async def test_reads_from_db(self, db_session, fake_redis, seed_templates):
        result = await get_push_templates(db_session, fake_redis)
        template = result.get_template("RIDE_ACCEPTED", "en")
        assert template is not None
        assert template.title == "New Ride"

    async def test_caches_to_redis(self, db_session, fake_redis, seed_templates):
        await get_push_templates(db_session, fake_redis)
        assert CACHE_KEY_PUSH_TEMPLATES in fake_redis._store

    async def test_reads_from_redis_cache(self, db_session, fake_redis, seed_templates):
        # Warm cache
        await get_push_templates(db_session, fake_redis)

        # Modify DB (won't be seen because cache is warm)
        cached_json = fake_redis._store[CACHE_KEY_PUSH_TEMPLATES]
        data = json.loads(cached_json)
        data["RIDE_ACCEPTED"]["en"]["title"] = "Modified Title"
        fake_redis._store[CACHE_KEY_PUSH_TEMPLATES] = json.dumps(data)

        result = await get_push_templates(db_session, fake_redis)
        template = result.get_template("RIDE_ACCEPTED", "en")
        assert template.title == "Modified Title"

    async def test_spanish_template_from_db(self, db_session, fake_redis, seed_templates):
        result = await get_push_templates(db_session, fake_redis)
        template = result.get_template("CREDITS_DEPLETED", "es")
        assert template is not None
        assert template.title == "CREDITS_DEPLETED ES title"


class TestInvalidatePushTemplates:
    """Tests for cache invalidation."""

    async def test_removes_from_redis(self, fake_redis):
        fake_redis._store[CACHE_KEY_PUSH_TEMPLATES] = "stale"
        await invalidate_push_templates(fake_redis)
        assert CACHE_KEY_PUSH_TEMPLATES not in fake_redis._store

    async def test_handles_missing_key(self, fake_redis):
        # Should not raise
        await invalidate_push_templates(fake_redis)
