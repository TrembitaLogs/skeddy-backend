import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import jwt
import pytest
from fastapi import HTTPException
from redis.exceptions import RedisError

from app.config import settings
from app.models.user import User
from app.services.auth_service import (
    create_access_token,
    create_refresh_token,
    decode_access_token,
    delete_refresh_token,
    delete_user_refresh_tokens,
    get_refresh_token_by_hash,
    hash_password,
    hash_refresh_token,
    refresh_tokens,
    save_refresh_token,
    verify_password,
)


def test_hash_password_returns_bcrypt_hash():
    hashed = hash_password("testpass")
    assert hashed.startswith("$2b$")


def test_verify_password_correct():
    hashed = hash_password("testpass")
    assert verify_password("testpass", hashed) is True


def test_verify_password_wrong():
    hashed = hash_password("testpass")
    assert verify_password("wrongpass", hashed) is False


def test_hash_password_uses_salt():
    hash1 = hash_password("testpass")
    hash2 = hash_password("testpass")
    assert hash1 != hash2


# --- JWT token tests ---


def test_create_access_token_returns_valid_jwt_with_sub():
    user_id = uuid4()
    token = create_access_token(user_id)

    payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    assert payload["sub"] == str(user_id)
    assert "exp" in payload
    assert "iat" in payload


def test_decode_access_token_returns_payload():
    user_id = uuid4()
    token = create_access_token(user_id)

    payload = decode_access_token(token)
    assert payload is not None
    assert payload["sub"] == str(user_id)


def test_decode_access_token_expired_returns_none():
    expired_payload = {
        "sub": str(uuid4()),
        "exp": datetime.now(UTC) - timedelta(seconds=1),
        "iat": datetime.now(UTC) - timedelta(hours=25),
    }
    token = jwt.encode(expired_payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

    assert decode_access_token(token) is None


def test_decode_access_token_invalid_returns_none():
    assert decode_access_token("not-a-valid-token") is None


def test_create_refresh_token_returns_urlsafe_string():
    token = create_refresh_token()
    # secrets.token_urlsafe(32) produces ~43 base64url chars
    assert len(token) > 36
    assert isinstance(token, str)


# --- Refresh token hash & DB tests ---


def test_hash_refresh_token_returns_64_char_hex():
    result = hash_refresh_token("some-test-token")
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_hash_refresh_token_is_deterministic():
    token = "test-token-123"
    assert hash_refresh_token(token) == hash_refresh_token(token)


async def test_save_refresh_token_creates_record(db_session):
    user = User(email="test@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    token = "my-refresh-token"
    expires_at = datetime.now(UTC) + timedelta(days=30)

    record = await save_refresh_token(db_session, user.id, token, expires_at)

    assert record.id is not None
    assert record.user_id == user.id
    assert record.token_hash == hash_refresh_token(token)
    assert record.expires_at is not None


async def test_get_refresh_token_by_hash_found(db_session):
    user = User(email="test@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    token = "find-me-token"
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, token, expires_at)

    token_hash = hash_refresh_token(token)
    record = await get_refresh_token_by_hash(db_session, token_hash)

    assert record is not None
    assert record.token_hash == token_hash
    assert record.user_id == user.id


async def test_get_refresh_token_by_hash_not_found(db_session):
    result = await get_refresh_token_by_hash(db_session, "a" * 64)
    assert result is None


async def test_delete_refresh_token_removes_record(db_session):
    user = User(email="test@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    token = "delete-me-token"
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, token, expires_at)

    token_hash = hash_refresh_token(token)
    await delete_refresh_token(db_session, token_hash)

    record = await get_refresh_token_by_hash(db_session, token_hash)
    assert record is None


async def test_delete_user_refresh_tokens_removes_all(db_session):
    user = User(email="test@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, "token-1", expires_at)
    await save_refresh_token(db_session, user.id, "token-2", expires_at)
    await save_refresh_token(db_session, user.id, "token-3", expires_at)

    count = await delete_user_refresh_tokens(db_session, user.id)
    assert count == 3

    for t in ["token-1", "token-2", "token-3"]:
        record = await get_refresh_token_by_hash(db_session, hash_refresh_token(t))
        assert record is None


async def test_delete_user_refresh_tokens_returns_zero_when_none(db_session):
    user = User(email="test@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    count = await delete_user_refresh_tokens(db_session, user.id)
    assert count == 0


# --- refresh_tokens() with Redis grace period ---


def _make_fake_redis():
    """Create a Redis mock that stores values in memory."""
    store = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_setex(key, ttl, value):
        store[key] = value

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis._store = store
    return redis


def _make_broken_redis():
    """Create a Redis mock that always raises RedisError."""
    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=RedisError("Connection refused"))
    redis.setex = AsyncMock(side_effect=RedisError("Connection refused"))
    return redis


async def test_refresh_tokens_valid_token_returns_new_pair(db_session):
    fake_redis = _make_fake_redis()
    user = User(email="rt@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    old_token = create_refresh_token()
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, old_token, expires_at)

    result = await refresh_tokens(db_session, fake_redis, old_token)

    assert result["user_id"] == str(user.id)
    assert result["refresh_token"] != old_token

    # Verify access token is a valid JWT with correct user_id
    payload = decode_access_token(result["access_token"])
    assert payload is not None
    assert payload["sub"] == str(user.id)

    # Verify old token is deleted from DB
    old_hash = hash_refresh_token(old_token)
    assert await get_refresh_token_by_hash(db_session, old_hash) is None

    # Verify new token is saved in DB
    new_hash = hash_refresh_token(result["refresh_token"])
    new_record = await get_refresh_token_by_hash(db_session, new_hash)
    assert new_record is not None
    assert new_record.user_id == user.id


async def test_refresh_tokens_grace_period_returns_cached_result(db_session):
    fake_redis = _make_fake_redis()
    user = User(email="rt@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    old_token = create_refresh_token()
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, old_token, expires_at)

    # First call - normal refresh (rotates tokens, caches result)
    result1 = await refresh_tokens(db_session, fake_redis, old_token)

    # Second call - same old token, should return cached result from Redis
    result2 = await refresh_tokens(db_session, fake_redis, old_token)

    assert result1 == result2


async def test_refresh_tokens_invalid_token_raises_401(db_session):
    fake_redis = _make_fake_redis()

    with pytest.raises(HTTPException) as exc_info:
        await refresh_tokens(db_session, fake_redis, "nonexistent-token")

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_REFRESH_TOKEN"


async def test_refresh_tokens_expired_token_raises_401(db_session):
    fake_redis = _make_fake_redis()
    user = User(email="rt@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    old_token = create_refresh_token()
    expires_at = datetime.now(UTC) - timedelta(days=1)
    await save_refresh_token(db_session, user.id, old_token, expires_at)

    with pytest.raises(HTTPException) as exc_info:
        await refresh_tokens(db_session, fake_redis, old_token)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "REFRESH_TOKEN_EXPIRED"

    # Verify expired token was deleted from DB
    old_hash = hash_refresh_token(old_token)
    assert await get_refresh_token_by_hash(db_session, old_hash) is None


async def test_refresh_tokens_caches_with_10s_ttl(db_session):
    fake_redis = _make_fake_redis()
    user = User(email="rt@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    old_token = create_refresh_token()
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, old_token, expires_at)

    result = await refresh_tokens(db_session, fake_redis, old_token)

    old_hash = hash_refresh_token(old_token)
    cache_key = f"refresh_grace:{old_hash}"
    fake_redis.setex.assert_called_once_with(cache_key, 10, json.dumps(result))


async def test_refresh_tokens_works_without_redis(db_session):
    broken_redis = _make_broken_redis()
    user = User(email="rt@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    old_token = create_refresh_token()
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, old_token, expires_at)

    # Should not raise - Redis errors are swallowed
    result = await refresh_tokens(db_session, broken_redis, old_token)

    assert result["user_id"] == str(user.id)
    assert "access_token" in result
    assert "refresh_token" in result


async def test_refresh_tokens_no_grace_period_when_redis_down(db_session):
    broken_redis = _make_broken_redis()
    user = User(email="rt@example.com", password_hash="fakehash")
    db_session.add(user)
    await db_session.flush()

    old_token = create_refresh_token()
    expires_at = datetime.now(UTC) + timedelta(days=30)
    await save_refresh_token(db_session, user.id, old_token, expires_at)

    # First call succeeds (Redis errors swallowed, token rotated in DB)
    result = await refresh_tokens(db_session, broken_redis, old_token)
    assert result["user_id"] == str(user.id)

    # Second call fails - old token deleted from DB, Redis can't serve cache
    with pytest.raises(HTTPException) as exc_info:
        await refresh_tokens(db_session, broken_redis, old_token)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "INVALID_REFRESH_TOKEN"
