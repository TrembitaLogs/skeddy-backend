"""Tests for POST /auth/reset-password (code-based flow).

Test strategy from task 15:
1. POST with valid {email, code, new_password} → 200 OK
2. Verify password changed (login with new password succeeds)
3. Verify all refresh tokens invalidated
4. POST with wrong code → 401 INVALID_RESET_CODE, attempts incremented
5. POST 5 times with wrong code → code invalidated
6. POST with expired code (15 min TTL) → 401
7. POST with too-short password → 422 VALIDATION_ERROR
"""

import hashlib
import json
from unittest.mock import AsyncMock, patch

from redis.exceptions import RedisError

REGISTER_URL = "/api/v1/auth/register"
LOGIN_URL = "/api/v1/auth/login"
REFRESH_URL = "/api/v1/auth/refresh"
RESET_PASSWORD_URL = "/api/v1/auth/reset-password"
REQUEST_RESET_URL = "/api/v1/auth/request-reset"

_TEST_PASSWORD = "securePass1"
_NEW_PASSWORD = "newSecurePass2"
_TEST_EMAIL = "resetpwd@example.com"
_RESET_CODE = "84729123"


async def _register_user(app_client, email=_TEST_EMAIL):
    """Helper: register a user via API and return response data."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


async def _store_reset_code_in_redis(fake_redis, email, code=_RESET_CODE):
    """Helper: store a reset code directly in Redis (bypasses request-reset endpoint)."""
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    await fake_redis.setex(
        f"reset_code:{email}",
        900,
        json.dumps({"code_hash": code_hash, "attempts": 0}),
    )


# --- Test Strategy: 1. POST with valid {email, code, new_password} → 200 OK ---


async def test_reset_password_valid_code_returns_200(app_client, fake_redis):
    """POST /auth/reset-password with valid code → 200 {"ok": true}."""
    await _register_user(app_client)
    await _store_reset_code_in_redis(fake_redis, _TEST_EMAIL)

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": _TEST_EMAIL, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy: 2. Verify password changed (login with new password) ---


async def test_reset_password_new_password_works_for_login(app_client, fake_redis):
    """After password reset, login with new password should succeed."""
    email = "login-after-reset@example.com"
    await _register_user(app_client, email=email)
    await _store_reset_code_in_redis(fake_redis, email)

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": email, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )
    assert response.status_code == 200

    login_response = await app_client.post(
        LOGIN_URL,
        json={"email": email, "password": _NEW_PASSWORD},
    )
    assert login_response.status_code == 200
    assert "access_token" in login_response.json()


# --- Test Strategy: 3. Verify all refresh tokens invalidated ---


async def test_reset_password_invalidates_refresh_tokens(app_client, fake_redis):
    """After successful password reset, old refresh tokens must be invalid."""
    email = "rt-invalidate@example.com"
    reg = await _register_user(app_client, email=email)
    await _store_reset_code_in_redis(fake_redis, email)

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": email, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )
    assert response.status_code == 200

    # Try to refresh with old token — should fail
    refresh_response = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": reg["refresh_token"]},
    )
    assert refresh_response.status_code == 401


# --- Test Strategy: 4. POST with wrong code → 401, attempts incremented ---


async def test_reset_password_wrong_code_returns_401(app_client, fake_redis):
    """POST /auth/reset-password with wrong code → 401 INVALID_RESET_CODE."""
    await _register_user(app_client, email="wrongcode@example.com")
    await _store_reset_code_in_redis(fake_redis, "wrongcode@example.com")

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": "wrongcode@example.com", "code": "00000000", "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_RESET_CODE"

    # Verify attempts incremented in Redis
    stored = await fake_redis.get("reset_code:wrongcode@example.com")
    data = json.loads(stored)
    assert data["attempts"] == 1


# --- Test Strategy: 5. POST 5 times with wrong code → code invalidated ---


async def test_reset_password_5_wrong_attempts_invalidates_code(app_client, fake_redis):
    """After 5 wrong attempts, the code is invalidated.

    Uses time mock to bypass exponential backoff between attempts.
    """
    email = "maxattempts@example.com"
    await _register_user(app_client, email=email)
    await _store_reset_code_in_redis(fake_redis, email)

    # Mock time.time to advance by 1000s on each call, ensuring the
    # exponential backoff window (max 60s) is always satisfied.
    call_count = 0

    def advancing_time():
        nonlocal call_count
        call_count += 1
        return call_count * 1000.0

    with patch("app.services.auth_service.time") as mock_time:
        mock_time.time.side_effect = advancing_time

        # Make 5 wrong attempts
        for _i in range(5):
            response = await app_client.post(
                RESET_PASSWORD_URL,
                json={"email": email, "code": "00000000", "new_password": _NEW_PASSWORD},
            )
            assert response.status_code == 401

        # 6th attempt with correct code should also fail — code is gone
        response = await app_client.post(
            RESET_PASSWORD_URL,
            json={"email": email, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
        )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_RESET_CODE"


# --- Test Strategy: 6. POST with expired code (TTL expired) → 401 ---


async def test_reset_password_expired_code_returns_401(app_client, fake_redis):
    """POST /auth/reset-password with expired code → 401."""
    await _register_user(app_client, email="expired@example.com")
    await _store_reset_code_in_redis(fake_redis, "expired@example.com")

    # Simulate TTL expiration by deleting the code from Redis
    await fake_redis.delete("reset_code:expired@example.com")

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": "expired@example.com", "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_RESET_CODE"


# --- Test Strategy: 7. POST with too-short password → 422 ---


async def test_reset_password_short_password_returns_422(app_client, fake_redis):
    """POST /auth/reset-password with new_password < 8 chars → 422."""
    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": "short@example.com", "code": "12345678", "new_password": "short"},
    )

    assert response.status_code == 422


# --- Additional: Redis down → 503 SERVICE_UNAVAILABLE ---


async def test_reset_password_redis_unavailable_returns_503(app_client, fake_redis):
    """POST /auth/reset-password when Redis is down → 503."""
    fake_redis.ping = AsyncMock(side_effect=RedisError("Connection refused"))

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": "any@example.com", "code": "12345678", "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


# --- Additional: old password no longer works after reset ---


async def test_reset_password_old_password_no_longer_works(app_client, fake_redis):
    """After password reset, login with old password should fail."""
    email = "old-pwd-fail@example.com"
    await _register_user(app_client, email=email)
    await _store_reset_code_in_redis(fake_redis, email)

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": email, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )
    assert response.status_code == 200

    login_response = await app_client.post(
        LOGIN_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert login_response.status_code == 401


# --- Additional: code is consumed after successful reset ---


async def test_reset_password_code_consumed_after_success(app_client, fake_redis):
    """After successful reset, the same code cannot be reused."""
    email = "reuse-code@example.com"
    await _register_user(app_client, email=email)
    await _store_reset_code_in_redis(fake_redis, email)

    # First use — should succeed
    response1 = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": email, "code": _RESET_CODE, "new_password": _NEW_PASSWORD},
    )
    assert response1.status_code == 200

    # Second use — code already consumed
    response2 = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": email, "code": _RESET_CODE, "new_password": "anotherPassword3"},
    )
    assert response2.status_code == 401
    assert response2.json()["error"]["code"] == "INVALID_RESET_CODE"


# --- Additional: invalid code format → 422 ---


async def test_reset_password_invalid_code_format_returns_422(app_client):
    """POST /auth/reset-password with non-6-digit code → 422."""
    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"email": "fmt@example.com", "code": "abc", "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 422
