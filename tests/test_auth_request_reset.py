import hashlib
from unittest.mock import AsyncMock, patch

from redis.exceptions import RedisError

REGISTER_URL = "/api/v1/auth/register"
REQUEST_RESET_URL = "/api/v1/auth/request-reset"

_TEST_PASSWORD = "securePass1"
_TEST_EMAIL = "reset@example.com"


async def _register_user(app_client, email=_TEST_EMAIL):
    """Helper: register a user via API and return response data."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


# --- Test Strategy: 1. Existing email → 200, email sent (mock) ---


@patch("app.routers.auth.send_reset_email", new_callable=AsyncMock)
async def test_request_reset_existing_email_sends_email(mock_send, app_client):
    """POST /auth/request-reset with existing email → 200, email sent."""
    await _register_user(app_client)

    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": _TEST_EMAIL},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    mock_send.assert_awaited_once()
    call_args = mock_send.call_args
    assert call_args[0][0] == _TEST_EMAIL
    # Second arg is the plain-text token (UUID)
    assert len(call_args[0][1]) == 36  # UUID format


# --- Test Strategy: 2. Non-existent email → 200 (enumeration protection) ---


@patch("app.routers.auth.send_reset_email", new_callable=AsyncMock)
async def test_request_reset_nonexistent_email_returns_200(mock_send, app_client):
    """POST /auth/request-reset with non-existent email → 200 (no email sent)."""
    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "nobody@example.com"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    mock_send.assert_not_awaited()


# --- Test Strategy: 3. Invalid email format → 422 validation error ---


async def test_request_reset_invalid_email_format_returns_422(app_client):
    """POST /auth/request-reset with invalid email → 422."""
    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "not-an-email"},
    )

    assert response.status_code == 422


# --- Test Strategy: 4. Verify reset_token stored in Redis with correct TTL ---


@patch("app.routers.auth.send_reset_email", new_callable=AsyncMock)
async def test_request_reset_stores_token_in_redis(mock_send, app_client, fake_redis):
    """POST /auth/request-reset stores SHA256 hashed token in Redis."""
    reg = await _register_user(app_client, email="redis-check@example.com")
    user_id = reg["user_id"]

    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "redis-check@example.com"},
    )
    assert response.status_code == 200

    # Extract the plain token from the send_reset_email call
    plain_token = mock_send.call_args[0][1]
    token_hash = hashlib.sha256(plain_token.encode()).hexdigest()

    # Verify the token is stored in Redis
    stored_user_id = await fake_redis.get(f"reset_token:{token_hash}")
    assert stored_user_id == user_id

    # Verify user_reset tracking key exists
    stored_hash = await fake_redis.get(f"user_reset:{user_id}")
    assert stored_hash == token_hash

    # Verify setex was called with correct TTL (3600 seconds)
    setex_calls = fake_redis.setex.call_args_list
    token_setex = [c for c in setex_calls if f"reset_token:{token_hash}" in str(c)]
    assert len(token_setex) == 1
    assert token_setex[0][0][1] == 3600


# --- Test Strategy: 5. Old token invalidation on second request ---


@patch("app.routers.auth.send_reset_email", new_callable=AsyncMock)
async def test_request_reset_invalidates_previous_token(mock_send, app_client, fake_redis):
    """Second request-reset invalidates the previous reset token."""
    await _register_user(app_client, email="invalidate@example.com")

    # First request
    await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "invalidate@example.com"},
    )
    first_token = mock_send.call_args[0][1]
    first_hash = hashlib.sha256(first_token.encode()).hexdigest()

    # Verify first token exists
    assert await fake_redis.get(f"reset_token:{first_hash}") is not None

    # Second request
    await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "invalidate@example.com"},
    )
    second_token = mock_send.call_args[0][1]
    second_hash = hashlib.sha256(second_token.encode()).hexdigest()

    # First token should be invalidated
    assert await fake_redis.get(f"reset_token:{first_hash}") is None
    # Second token should be stored
    assert await fake_redis.get(f"reset_token:{second_hash}") is not None


# --- Test Strategy: 6. Redis down → 503 SERVICE_UNAVAILABLE ---


async def test_request_reset_redis_unavailable_returns_503(app_client, fake_redis):
    """POST /auth/request-reset when Redis is down → 503."""
    fake_redis.ping = AsyncMock(side_effect=RedisError("Connection refused"))

    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": _TEST_EMAIL},
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


# --- Test Strategy: 7. SMTP failure → still 200 ---


@patch(
    "app.routers.auth.send_reset_email",
    new_callable=AsyncMock,
    side_effect=Exception("SMTP connection failed"),
)
async def test_request_reset_smtp_failure_still_returns_200(mock_send, app_client):
    """POST /auth/request-reset returns 200 even if SMTP fails."""
    await _register_user(app_client, email="smtp-fail@example.com")

    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "smtp-fail@example.com"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    mock_send.assert_awaited_once()


# --- Additional: missing email field → 422 ---


async def test_request_reset_missing_email_field_returns_422(app_client):
    """POST /auth/request-reset without email field → 422."""
    response = await app_client.post(
        REQUEST_RESET_URL,
        json={},
    )

    assert response.status_code == 422
