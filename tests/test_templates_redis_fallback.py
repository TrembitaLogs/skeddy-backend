"""Tests for Redis fallback paths in templates.py (push and email).

Covers the untested resolution paths:
- Redis unavailable -> memory cache hit
- Redis unavailable -> memory cache miss -> DB
- Redis returns malformed JSON -> fallback
- All sources fail -> hardcoded defaults
- Redis cache write-back after DB fetch
- Memory cache population after successful fetch
"""

import json
from unittest.mock import AsyncMock

import pytest_asyncio
from redis.exceptions import RedisError

from app.models.email_template import EmailTemplate as EmailTemplateModel
from app.models.push_template import PushTemplate as PushTemplateModel
from app.schemas.push_templates import PushNotificationTemplatesConfig
from app.services.config_service.cache import (
    CACHE_KEY_EMAIL_TEMPLATES,
    CACHE_KEY_PUSH_TEMPLATES,
    _memory_cache,
)
from app.services.config_service.templates import (
    DEFAULT_PUSH_TEMPLATES,
    get_email_templates,
    get_push_templates,
    invalidate_email_templates,
    invalidate_push_templates,
)


def _broken_redis():
    """Create a Redis mock that raises RedisError on all operations."""
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=RedisError("connection refused"))
    redis.setex = AsyncMock(side_effect=RedisError("connection refused"))
    redis.delete = AsyncMock(side_effect=RedisError("connection refused"))
    return redis


@pytest_asyncio.fixture
async def seed_push_templates(db_session):
    """Seed push_templates table with test data."""
    for ntype, langs in DEFAULT_PUSH_TEMPLATES.items():
        row = PushTemplateModel(
            notification_type=ntype,
            title_en=langs["en"]["title"],
            body_en=langs["en"]["body"],
            title_es=langs["es"]["title"],
            body_es=langs["es"]["body"],
        )
        db_session.add(row)
    await db_session.flush()


@pytest_asyncio.fixture
async def seed_email_templates(db_session):
    """Seed email_templates table with test data."""
    row = EmailTemplateModel(
        email_type="WELCOME",
        subject_en="Welcome!",
        body_en="Welcome to Skeddy.",
        subject_es="Bienvenido!",
        body_es="Bienvenido a Skeddy.",
    )
    db_session.add(row)
    await db_session.flush()


# ===========================================================================
# get_push_templates — Redis fallback paths
# ===========================================================================


class TestPushTemplatesRedisUnavailableMemoryHit:
    """Redis raises RedisError, memory cache has a value -> return from memory."""

    async def test_returns_from_memory_cache(self, db_session, fake_redis):
        # Warm memory cache via a successful Redis+DB fetch first
        await get_push_templates(db_session, fake_redis)
        cached_value = _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES)
        assert cached_value is not None

        # Now Redis is down -> should return from memory
        broken = _broken_redis()
        result = await get_push_templates(db_session, broken)
        assert isinstance(result, PushNotificationTemplatesConfig)
        assert "RIDE_ACCEPTED" in result.root

    async def test_memory_hit_skips_db(self, db_session, fake_redis, seed_push_templates):
        # Warm cache
        await get_push_templates(db_session, fake_redis)

        # Redis down -> memory hit should skip DB entirely
        broken = _broken_redis()
        result = await get_push_templates(db_session, broken)
        assert isinstance(result, PushNotificationTemplatesConfig)
        template = result.get_template("RIDE_ACCEPTED", "en")
        assert template is not None
        assert template.title == "New Ride"


class TestPushTemplatesRedisUnavailableMemoryMissDbAvailable:
    """Redis down, memory empty, DB has templates -> fetch from DB."""

    async def test_falls_back_to_db(self, db_session, seed_push_templates):
        broken = _broken_redis()
        result = await get_push_templates(db_session, broken)
        assert isinstance(result, PushNotificationTemplatesConfig)
        template = result.get_template("RIDE_ACCEPTED", "en")
        assert template is not None
        assert template.title == "New Ride"


class TestPushTemplatesMalformedRedisJson:
    """Redis returns invalid JSON -> graceful fallback to defaults."""

    async def test_malformed_json_falls_back_to_defaults(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_PUSH_TEMPLATES] = "not-valid-json{{"
        result = await get_push_templates(db_session, fake_redis)
        assert isinstance(result, PushNotificationTemplatesConfig)
        # Should fall back to DEFAULT_PUSH_TEMPLATES
        assert "RIDE_ACCEPTED" in result.root

    async def test_invalid_structure_falls_back_to_defaults(self, db_session, fake_redis):
        # Valid JSON but invalid structure for PushNotificationTemplatesConfig
        fake_redis._store[CACHE_KEY_PUSH_TEMPLATES] = json.dumps({"UNKNOWN": "bad"})
        result = await get_push_templates(db_session, fake_redis)
        assert isinstance(result, PushNotificationTemplatesConfig)
        # Falls back to defaults because validation fails
        assert "RIDE_ACCEPTED" in result.root


class TestPushTemplatesAllSourcesFail:
    """Redis down, memory empty, DB empty -> hardcoded defaults."""

    async def test_returns_hardcoded_defaults(self, db_session):
        broken = _broken_redis()
        result = await get_push_templates(db_session, broken)
        assert isinstance(result, PushNotificationTemplatesConfig)
        assert len(result.root) == len(DEFAULT_PUSH_TEMPLATES)
        for ntype in DEFAULT_PUSH_TEMPLATES:
            assert ntype in result.root


class TestPushTemplatesCacheWriteBack:
    """After fetching from DB, template is written back to Redis."""

    async def test_db_fetch_writes_to_redis(self, db_session, fake_redis, seed_push_templates):
        # Redis empty, DB has data -> should write back to Redis
        result = await get_push_templates(db_session, fake_redis)
        assert isinstance(result, PushNotificationTemplatesConfig)
        assert CACHE_KEY_PUSH_TEMPLATES in fake_redis._store

        # Verify cached JSON is valid
        cached_json = fake_redis._store[CACHE_KEY_PUSH_TEMPLATES]
        parsed = json.loads(cached_json)
        assert "RIDE_ACCEPTED" in parsed

    async def test_db_fetch_redis_writeback_failure_is_silent(
        self, db_session, seed_push_templates
    ):
        """Redis write-back fails silently after DB fetch."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)  # Cache miss
        redis.setex = AsyncMock(side_effect=RedisError("write failed"))

        result = await get_push_templates(db_session, redis)
        assert isinstance(result, PushNotificationTemplatesConfig)
        template = result.get_template("RIDE_ACCEPTED", "en")
        assert template is not None


class TestPushTemplatesMemoryCachePopulation:
    """Memory cache is populated after successful fetch."""

    async def test_redis_hit_populates_memory(self, db_session, fake_redis, seed_push_templates):
        # First: get via DB to populate Redis
        await get_push_templates(db_session, fake_redis)

        # Clear memory cache
        _memory_cache.pop(CACHE_KEY_PUSH_TEMPLATES, None)
        assert _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES) is None

        # Second fetch: Redis hit -> should repopulate memory
        await get_push_templates(db_session, fake_redis)
        assert _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES) is not None

    async def test_db_hit_populates_memory(self, db_session, fake_redis, seed_push_templates):
        assert _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES) is None

        await get_push_templates(db_session, fake_redis)

        cached = _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES)
        assert cached is not None
        assert isinstance(cached, PushNotificationTemplatesConfig)


# ===========================================================================
# get_email_templates — Redis fallback paths
# ===========================================================================


class TestEmailTemplatesRedisUnavailableMemoryHit:
    """Redis raises RedisError, memory cache has a value -> return from memory."""

    async def test_returns_from_memory_cache(self, db_session, fake_redis, seed_email_templates):
        # Warm memory cache via successful fetch
        await get_email_templates(db_session, fake_redis)
        cached_value = _memory_cache.get(CACHE_KEY_EMAIL_TEMPLATES)
        assert cached_value is not None

        # Redis down -> should return from memory
        broken = _broken_redis()
        result = await get_email_templates(db_session, broken)
        assert isinstance(result, dict)
        assert "WELCOME" in result


class TestEmailTemplatesRedisUnavailableMemoryMissDbAvailable:
    """Redis down, memory empty, DB has templates -> fetch from DB."""

    async def test_falls_back_to_db(self, db_session, seed_email_templates):
        broken = _broken_redis()
        result = await get_email_templates(db_session, broken)
        assert isinstance(result, dict)
        assert "WELCOME" in result
        assert result["WELCOME"]["en"]["subject"] == "Welcome!"


class TestEmailTemplatesMalformedRedisJson:
    """Redis returns invalid JSON -> graceful fallback to empty dict."""

    async def test_malformed_json_returns_empty(self, db_session, fake_redis):
        fake_redis._store[CACHE_KEY_EMAIL_TEMPLATES] = "not-valid-json{{"
        result = await get_email_templates(db_session, fake_redis)
        assert result == {}


class TestEmailTemplatesAllSourcesFail:
    """Redis down, memory empty, DB empty -> returns empty dict."""

    async def test_returns_empty_dict(self, db_session):
        broken = _broken_redis()
        result = await get_email_templates(db_session, broken)
        assert result == {}


class TestEmailTemplatesCacheWriteBack:
    """After fetching from DB, template is written back to Redis."""

    async def test_db_fetch_writes_to_redis(self, db_session, fake_redis, seed_email_templates):
        result = await get_email_templates(db_session, fake_redis)
        assert "WELCOME" in result
        assert CACHE_KEY_EMAIL_TEMPLATES in fake_redis._store

        cached_json = fake_redis._store[CACHE_KEY_EMAIL_TEMPLATES]
        parsed = json.loads(cached_json)
        assert "WELCOME" in parsed

    async def test_db_fetch_redis_writeback_failure_is_silent(
        self, db_session, seed_email_templates
    ):
        """Redis write-back fails silently after DB fetch."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock(side_effect=RedisError("write failed"))

        result = await get_email_templates(db_session, redis)
        assert isinstance(result, dict)
        assert "WELCOME" in result


class TestEmailTemplatesMemoryCachePopulation:
    """Memory cache is populated after successful fetch."""

    async def test_redis_hit_populates_memory(self, db_session, fake_redis, seed_email_templates):
        await get_email_templates(db_session, fake_redis)
        _memory_cache.pop(CACHE_KEY_EMAIL_TEMPLATES, None)

        await get_email_templates(db_session, fake_redis)
        assert _memory_cache.get(CACHE_KEY_EMAIL_TEMPLATES) is not None

    async def test_db_hit_populates_memory(self, db_session, fake_redis, seed_email_templates):
        assert _memory_cache.get(CACHE_KEY_EMAIL_TEMPLATES) is None
        await get_email_templates(db_session, fake_redis)
        assert _memory_cache.get(CACHE_KEY_EMAIL_TEMPLATES) is not None


# ===========================================================================
# Invalidation with Redis errors
# ===========================================================================


class TestInvalidatePushTemplatesRedisError:
    """invalidate_push_templates handles Redis errors gracefully."""

    async def test_clears_memory_even_when_redis_fails(self):
        _memory_cache[CACHE_KEY_PUSH_TEMPLATES] = "stale"
        broken = _broken_redis()

        await invalidate_push_templates(broken)

        assert _memory_cache.get(CACHE_KEY_PUSH_TEMPLATES) is None

    async def test_redis_delete_error_is_silent(self):
        broken = _broken_redis()
        # Should not raise
        await invalidate_push_templates(broken)


class TestInvalidateEmailTemplatesRedisError:
    """invalidate_email_templates handles Redis errors gracefully."""

    async def test_clears_memory_even_when_redis_fails(self):
        _memory_cache[CACHE_KEY_EMAIL_TEMPLATES] = "stale"
        broken = _broken_redis()

        await invalidate_email_templates(broken)

        assert _memory_cache.get(CACHE_KEY_EMAIL_TEMPLATES) is None

    async def test_redis_delete_error_is_silent(self):
        broken = _broken_redis()
        # Should not raise
        await invalidate_email_templates(broken)
