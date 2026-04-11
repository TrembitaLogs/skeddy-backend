"""Tests for account lockout (S-05): progressive lockout per email after failed logins."""

from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from app.services.auth_service import (
    check_account_lockout,
    clear_login_attempts,
    record_failed_login,
)


def _make_redis(store: dict[str, str] | None = None):
    """Build a minimal fake Redis for lockout tests."""
    if store is None:
        store = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_incr(key):
        current = int(store.get(key, "0"))
        store[key] = str(current + 1)
        return current + 1

    async def mock_expire(key, ttl):
        return 1 if key in store else 0

    async def mock_ttl(key):
        return 900 if key in store else -2

    async def mock_delete(*keys):
        for key in keys:
            store.pop(key, None)

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.incr = AsyncMock(side_effect=mock_incr)
    redis.expire = AsyncMock(side_effect=mock_expire)
    redis.ttl = AsyncMock(side_effect=mock_ttl)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis._store = store
    return redis


class TestCheckAccountLockout:
    """Tests for check_account_lockout function."""

    @pytest.mark.asyncio
    async def test_no_lockout_when_no_attempts(self):
        """No exception when email has no failed attempts."""
        redis = _make_redis()
        await check_account_lockout(redis, "user@example.com")

    @pytest.mark.asyncio
    async def test_no_lockout_below_threshold(self):
        """No exception when attempts below first threshold (5)."""
        redis = _make_redis({"login_attempts:user@example.com": "4"})
        await check_account_lockout(redis, "user@example.com")

    @pytest.mark.asyncio
    async def test_lockout_at_first_threshold(self):
        """HTTPException(429) raised at 5 failed attempts."""
        redis = _make_redis({"login_attempts:user@example.com": "5"})
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await check_account_lockout(redis, "user@example.com")
        assert exc_info.value.status_code == 429
        assert exc_info.value.detail == "ACCOUNT_LOCKED"

    @pytest.mark.asyncio
    async def test_lockout_at_second_threshold(self):
        """HTTPException(429) raised at 10 failed attempts."""
        redis = _make_redis({"login_attempts:user@example.com": "10"})
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await check_account_lockout(redis, "user@example.com")
        assert exc_info.value.status_code == 429

    @pytest.mark.asyncio
    async def test_no_lockout_when_ttl_expired(self):
        """No lockout when key exists but TTL is expired (ttl <= 0)."""
        store = {"login_attempts:user@example.com": "5"}

        async def mock_ttl(key):
            return -1  # TTL expired

        redis = _make_redis(store)
        redis.ttl = AsyncMock(side_effect=mock_ttl)
        await check_account_lockout(redis, "user@example.com")

    @pytest.mark.asyncio
    async def test_graceful_on_redis_failure(self):
        """Allows login when Redis is unavailable."""
        redis = AsyncMock()
        redis.get = AsyncMock(side_effect=RedisError("connection lost"))
        await check_account_lockout(redis, "user@example.com")


class TestRecordFailedLogin:
    """Tests for record_failed_login function."""

    @pytest.mark.asyncio
    async def test_increments_counter(self):
        """Failed login increments the Redis counter."""
        redis = _make_redis()
        await record_failed_login(redis, "user@example.com")
        assert redis._store["login_attempts:user@example.com"] == "1"

    @pytest.mark.asyncio
    async def test_sets_expire_on_counter(self):
        """Expire is called after incrementing."""
        redis = _make_redis()
        await record_failed_login(redis, "user@example.com")
        redis.expire.assert_called_once()

    @pytest.mark.asyncio
    async def test_graceful_on_redis_failure(self):
        """Does not raise when Redis is unavailable."""
        redis = AsyncMock()
        redis.incr = AsyncMock(side_effect=RedisError("connection lost"))
        await record_failed_login(redis, "user@example.com")


class TestClearLoginAttempts:
    """Tests for clear_login_attempts function."""

    @pytest.mark.asyncio
    async def test_clears_counter(self):
        """Successful login clears the counter."""
        redis = _make_redis({"login_attempts:user@example.com": "3"})
        await clear_login_attempts(redis, "user@example.com")
        assert "login_attempts:user@example.com" not in redis._store

    @pytest.mark.asyncio
    async def test_graceful_on_redis_failure(self):
        """Does not raise when Redis is unavailable."""
        redis = AsyncMock()
        redis.delete = AsyncMock(side_effect=RedisError("connection lost"))
        await clear_login_attempts(redis, "user@example.com")


class TestLockoutIntegration:
    """Integration tests via HTTP endpoints."""

    @pytest.mark.asyncio
    async def test_login_returns_429_when_locked(self, app_client, fake_redis):
        """Login endpoint returns 429 when account is locked."""
        fake_redis._store["login_attempts:locked@example.com"] = "5"
        resp = await app_client.post(
            "/api/v1/auth/login",
            json={"email": "locked@example.com", "password": "any"},
        )
        assert resp.status_code == 429

    @pytest.mark.asyncio
    async def test_failed_login_increments_counter(self, app_client, fake_redis, db_session):
        """Failed login attempt increments the lockout counter."""
        resp = await app_client.post(
            "/api/v1/auth/login",
            json={"email": "nouser@example.com", "password": "wrongpass"},
        )
        assert resp.status_code == 401
        assert "login_attempts:nouser@example.com" in fake_redis._store

    @pytest.mark.asyncio
    async def test_successful_login_clears_counter(self, app_client, fake_redis, db_session):
        """Successful login clears the lockout counter."""
        from app.models.user import User
        from app.services.auth_service import hash_password

        user = User(email="good@example.com", password_hash=hash_password("correct123"))
        db_session.add(user)
        await db_session.commit()

        fake_redis._store["login_attempts:good@example.com"] = "3"

        resp = await app_client.post(
            "/api/v1/auth/login",
            json={"email": "good@example.com", "password": "correct123"},
        )
        assert resp.status_code == 200
        assert "login_attempts:good@example.com" not in fake_redis._store
