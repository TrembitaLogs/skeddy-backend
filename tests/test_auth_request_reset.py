import hashlib
import json
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


# --- Test Strategy: 1. Existing email → 200, email sent with 6-digit code ---


@patch("app.routers.auth.send_password_reset_code", new_callable=AsyncMock)
async def test_request_reset_existing_email_sends_code(mock_send, app_client):
    """POST /auth/request-reset with existing email → 200, email sent with 6-digit code."""
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
    # Second arg is the 6-digit code
    code = call_args[0][1]
    assert len(code) == 6
    assert code.isdigit()
    assert 100000 <= int(code) <= 999999


# --- Test Strategy: 2. Verify Redis: reset_code:{email} with code_hash and attempts=0 ---


@patch("app.routers.auth.send_password_reset_code", new_callable=AsyncMock)
async def test_request_reset_stores_code_in_redis(mock_send, app_client, fake_redis):
    """POST /auth/request-reset stores SHA256 hashed code in Redis as JSON."""
    await _register_user(app_client, email="redis-check@example.com")

    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "redis-check@example.com"},
    )
    assert response.status_code == 200

    # Extract the plain code from the send call
    plain_code = mock_send.call_args[0][1]
    expected_hash = hashlib.sha256(plain_code.encode()).hexdigest()

    # Verify the code is stored in Redis under reset_code:{email}
    stored_value = await fake_redis.get("reset_code:redis-check@example.com")
    assert stored_value is not None
    data = json.loads(stored_value)
    assert data["code_hash"] == expected_hash
    assert data["attempts"] == 0

    # Verify setex was called with correct TTL (900 seconds)
    setex_calls = fake_redis.setex.call_args_list
    code_setex = [c for c in setex_calls if "reset_code:redis-check@example.com" in str(c)]
    assert len(code_setex) == 1
    assert code_setex[0][0][1] == 900


# --- Test Strategy: 3. Non-existent email → 200 (enumeration protection) ---


@patch("app.routers.auth.send_password_reset_code", new_callable=AsyncMock)
async def test_request_reset_nonexistent_email_returns_200(mock_send, app_client):
    """POST /auth/request-reset with non-existent email → 200 (no email sent)."""
    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "nobody@example.com"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    mock_send.assert_not_awaited()


# --- Test Strategy: 4. Repeat request → previous code overwritten in Redis ---


@patch("app.routers.auth.send_password_reset_code", new_callable=AsyncMock)
async def test_request_reset_overwrites_previous_code(mock_send, app_client, fake_redis):
    """Second request-reset overwrites the previous reset code in Redis."""
    await _register_user(app_client, email="invalidate@example.com")

    # First request
    await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "invalidate@example.com"},
    )
    first_code = mock_send.call_args[0][1]
    first_hash = hashlib.sha256(first_code.encode()).hexdigest()

    # Verify first code exists
    stored = await fake_redis.get("reset_code:invalidate@example.com")
    assert stored is not None
    assert json.loads(stored)["code_hash"] == first_hash

    # Second request
    await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "invalidate@example.com"},
    )
    second_code = mock_send.call_args[0][1]
    second_hash = hashlib.sha256(second_code.encode()).hexdigest()

    # Value should now contain the second code hash
    stored = await fake_redis.get("reset_code:invalidate@example.com")
    assert stored is not None
    data = json.loads(stored)
    assert data["code_hash"] == second_hash
    assert data["attempts"] == 0


# --- Test Strategy: 5. Invalid email format → 422 validation error ---


async def test_request_reset_invalid_email_format_returns_422(app_client):
    """POST /auth/request-reset with invalid email → 422."""
    response = await app_client.post(
        REQUEST_RESET_URL,
        json={"email": "not-an-email"},
    )

    assert response.status_code == 422


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


# --- SMTP failure → still 200 ---


@patch(
    "app.routers.auth.send_password_reset_code",
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


# --- Missing email field → 422 ---


async def test_request_reset_missing_email_field_returns_422(app_client):
    """POST /auth/request-reset without email field → 422."""
    response = await app_client.post(
        REQUEST_RESET_URL,
        json={},
    )

    assert response.status_code == 422
