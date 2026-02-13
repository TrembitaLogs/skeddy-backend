import hashlib
from unittest.mock import AsyncMock, patch

from redis.exceptions import RedisError

REGISTER_URL = "/api/v1/auth/register"
LOGIN_URL = "/api/v1/auth/login"
REFRESH_URL = "/api/v1/auth/refresh"
REQUEST_RESET_URL = "/api/v1/auth/request-reset"
RESET_PASSWORD_URL = "/api/v1/auth/reset-password"

_TEST_PASSWORD = "securePass1"
_NEW_PASSWORD = "newSecurePass2"
_TEST_EMAIL = "resetpwd@example.com"


async def _register_user(app_client, email=_TEST_EMAIL):
    """Helper: register a user via API and return response data."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


async def _request_reset_and_get_token(app_client, fake_redis, email=_TEST_EMAIL):
    """Helper: request password reset and extract the plain token from mock."""
    with patch("app.routers.auth.send_reset_email", new_callable=AsyncMock) as mock_send:
        await app_client.post(
            REQUEST_RESET_URL,
            json={"email": email},
        )
        return mock_send.call_args[0][1]


# --- Test Strategy: 1. POST with valid token → 200, password changed in DB ---


async def test_reset_password_valid_token_returns_200(app_client, fake_redis):
    """POST /auth/reset-password with valid token → 200 {"ok": true}."""
    await _register_user(app_client)
    reset_token = await _request_reset_and_get_token(app_client, fake_redis)

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy: 2. POST with invalid token → 401 INVALID_RESET_TOKEN ---


async def test_reset_password_invalid_token_returns_401(app_client):
    """POST /auth/reset-password with invalid token → 401."""
    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": "totally-invalid-token", "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_RESET_TOKEN"


# --- Test Strategy: 3. POST with expired token (TTL expired) → 401 ---


async def test_reset_password_expired_token_returns_401(app_client, fake_redis):
    """POST /auth/reset-password with expired token → 401.

    Simulated by generating a token then manually removing it from Redis.
    """
    await _register_user(app_client, email="expired@example.com")
    reset_token = await _request_reset_and_get_token(
        app_client, fake_redis, email="expired@example.com"
    )

    # Simulate TTL expiration by deleting the token from fake Redis
    token_hash = hashlib.sha256(reset_token.encode()).hexdigest()
    await fake_redis.delete(f"reset_token:{token_hash}")

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_RESET_TOKEN"


# --- Test Strategy: 4. POST with already used token (second request) → 401 ---


async def test_reset_password_reused_token_returns_401(app_client, fake_redis):
    """POST /auth/reset-password twice with same token → second returns 401."""
    await _register_user(app_client, email="reuse@example.com")
    reset_token = await _request_reset_and_get_token(
        app_client, fake_redis, email="reuse@example.com"
    )

    # First use — should succeed
    response1 = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": _NEW_PASSWORD},
    )
    assert response1.status_code == 200

    # Second use — token already consumed
    response2 = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": "anotherPassword3"},
    )
    assert response2.status_code == 401
    assert response2.json()["error"]["code"] == "INVALID_RESET_TOKEN"


# --- Test Strategy: 5. After successful reset, all refresh tokens deleted ---


async def test_reset_password_invalidates_refresh_tokens(app_client, fake_redis):
    """After successful password reset, old refresh tokens must be invalid."""
    reg = await _register_user(app_client, email="rt-invalidate@example.com")
    reset_token = await _request_reset_and_get_token(
        app_client, fake_redis, email="rt-invalidate@example.com"
    )

    # Reset password
    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": _NEW_PASSWORD},
    )
    assert response.status_code == 200

    # Try to refresh with old token — should fail
    refresh_response = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": reg["refresh_token"]},
    )
    assert refresh_response.status_code == 401


# --- Test Strategy: 6. New password works for login ---


async def test_reset_password_new_password_works_for_login(app_client, fake_redis):
    """After password reset, login with new password should succeed."""
    email = "login-after-reset@example.com"
    await _register_user(app_client, email=email)
    reset_token = await _request_reset_and_get_token(app_client, fake_redis, email=email)

    # Reset password
    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": _NEW_PASSWORD},
    )
    assert response.status_code == 200

    # Login with new password
    login_response = await app_client.post(
        LOGIN_URL,
        json={"email": email, "password": _NEW_PASSWORD},
    )
    assert login_response.status_code == 200
    assert "access_token" in login_response.json()


# --- Test Strategy: 7. POST with too short new_password → 422 ---


async def test_reset_password_short_password_returns_422(app_client, fake_redis):
    """POST /auth/reset-password with new_password < 8 chars → 422."""
    await _register_user(app_client, email="shortpwd@example.com")
    reset_token = await _request_reset_and_get_token(
        app_client, fake_redis, email="shortpwd@example.com"
    )

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": "short"},
    )

    assert response.status_code == 422


# --- Additional: Redis down → 503 SERVICE_UNAVAILABLE ---


async def test_reset_password_redis_unavailable_returns_503(app_client, fake_redis):
    """POST /auth/reset-password when Redis is down → 503."""
    fake_redis.ping = AsyncMock(side_effect=RedisError("Connection refused"))

    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": "any-token", "new_password": _NEW_PASSWORD},
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


# --- Additional: old password no longer works after reset ---


async def test_reset_password_old_password_no_longer_works(app_client, fake_redis):
    """After password reset, login with old password should fail."""
    email = "old-pwd-fail@example.com"
    await _register_user(app_client, email=email)
    reset_token = await _request_reset_and_get_token(app_client, fake_redis, email=email)

    # Reset password
    response = await app_client.post(
        RESET_PASSWORD_URL,
        json={"token": reset_token, "new_password": _NEW_PASSWORD},
    )
    assert response.status_code == 200

    # Login with old password — should fail
    login_response = await app_client.post(
        LOGIN_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert login_response.status_code == 401
