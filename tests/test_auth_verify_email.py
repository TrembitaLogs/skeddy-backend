"""Tests for POST /auth/verify-email (code-based email verification).

Test strategy from task 19:
1. POST /auth/verify-email with valid code → 200 OK
2. GET /auth/me → email_verified: true
3. Repeat POST /auth/verify-email → 400 ALREADY_VERIFIED
4. POST with wrong code → 401 INVALID_VERIFICATION_CODE, attempts++
5. POST 5 times with wrong code → code invalidated
6. POST with expired code (24h TTL) → 401
7. POST without JWT → 401 Unauthorized
"""

import hashlib
import json
from unittest.mock import AsyncMock, patch

from redis.exceptions import RedisError

REGISTER_URL = "/api/v1/auth/register"
VERIFY_EMAIL_URL = "/api/v1/auth/verify-email"
ME_URL = "/api/v1/auth/me"

_TEST_PASSWORD = "securePass1"
_TEST_EMAIL = "verify@example.com"
_VERIFY_CODE = "59381723"


async def _register_and_get_auth(app_client, email=_TEST_EMAIL):
    """Helper: register a user via API and return (response_data, auth_headers)."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    data = response.json()
    headers = {"Authorization": f"Bearer {data['access_token']}"}
    return data, headers


async def _store_verify_code_in_redis(fake_redis, user_id, code=_VERIFY_CODE):
    """Helper: store a verification code directly in Redis."""
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    await fake_redis.setex(
        f"verify_code:{user_id}",
        86400,
        json.dumps({"code_hash": code_hash, "attempts": 0}),
    )


# --- Test Strategy: 1. POST /auth/verify-email with valid code → 200 OK ---


async def test_verify_email_valid_code_returns_200(app_client, fake_redis):
    """POST /auth/verify-email with valid code → 200 {"ok": true}."""
    data, headers = await _register_and_get_auth(app_client)
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy: 2. GET /auth/me → email_verified: true ---


async def test_verify_email_me_returns_verified_true(app_client, fake_redis):
    """After verification, GET /auth/me should return email_verified: true."""
    data, headers = await _register_and_get_auth(app_client, email="me-verified@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    # Verify email
    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )
    assert response.status_code == 200

    # Check /me
    me_response = await app_client.get(ME_URL, headers=headers)
    assert me_response.status_code == 200
    assert me_response.json()["email_verified"] is True


async def test_unverified_user_me_returns_verified_false(app_client, fake_redis):
    """Before verification, GET /auth/me should return email_verified: false."""
    _data, headers = await _register_and_get_auth(app_client, email="unverified@example.com")

    me_response = await app_client.get(ME_URL, headers=headers)
    assert me_response.status_code == 200
    assert me_response.json()["email_verified"] is False


# --- Test Strategy: 3. Repeat POST /auth/verify-email → 400 ALREADY_VERIFIED ---


async def test_verify_email_already_verified_returns_400(app_client, fake_redis):
    """POST /auth/verify-email when already verified → 400 ALREADY_VERIFIED."""
    data, headers = await _register_and_get_auth(app_client, email="already@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    # First verification — success
    response1 = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )
    assert response1.status_code == 200

    # Second attempt — already verified
    response2 = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )
    assert response2.status_code == 400
    assert response2.json()["error"]["code"] == "ALREADY_VERIFIED"


# --- Test Strategy: 4. POST with wrong code → 401, attempts incremented ---


async def test_verify_email_wrong_code_returns_401(app_client, fake_redis):
    """POST /auth/verify-email with wrong code → 401 INVALID_VERIFICATION_CODE."""
    data, headers = await _register_and_get_auth(app_client, email="wrongcode@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": "00000000"},
        headers=headers,
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_VERIFICATION_CODE"

    # Verify attempts incremented in Redis
    stored = await fake_redis.get(f"verify_code:{data['user_id']}")
    code_data = json.loads(stored)
    assert code_data["attempts"] == 1


# --- Test Strategy: 5. POST 5 times with wrong code → code invalidated ---


async def test_verify_email_5_wrong_attempts_invalidates_code(app_client, fake_redis):
    """After 5 wrong attempts, the verification code is invalidated.

    Uses time mock to bypass exponential backoff between attempts.
    """
    data, headers = await _register_and_get_auth(app_client, email="maxattempts@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    # Mock time.time to always return a value far in the future relative to
    # last_failed_at, bypassing exponential backoff delays.
    with patch("app.services.auth_service.time") as mock_time:
        mock_time.time.return_value = 1e12

        # Make 5 wrong attempts
        for _i in range(5):
            response = await app_client.post(
                VERIFY_EMAIL_URL,
                json={"code": "00000000"},
                headers=headers,
            )
            assert response.status_code == 401

        # 6th attempt with correct code should also fail — code is gone
        response = await app_client.post(
            VERIFY_EMAIL_URL,
            json={"code": _VERIFY_CODE},
            headers=headers,
        )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_VERIFICATION_CODE"


# --- Test Strategy: 6. POST with expired code (24h TTL) → 401 ---


async def test_verify_email_expired_code_returns_401(app_client, fake_redis):
    """POST /auth/verify-email with expired code → 401."""
    data, headers = await _register_and_get_auth(app_client, email="expired@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    # Simulate TTL expiration by deleting the code from Redis
    await fake_redis.delete(f"verify_code:{data['user_id']}")

    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_VERIFICATION_CODE"


# --- Test Strategy: 7. POST without JWT → 401 Unauthorized ---


async def test_verify_email_no_jwt_returns_401(app_client):
    """POST /auth/verify-email without Authorization header → 401."""
    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": "12345678"},
    )

    assert response.status_code == 401


# --- Additional: Redis down → 503 SERVICE_UNAVAILABLE ---


async def test_verify_email_redis_unavailable_returns_503(app_client, fake_redis):
    """POST /auth/verify-email when Redis is down → 503."""
    _data, headers = await _register_and_get_auth(app_client, email="redis-down@example.com")

    fake_redis.ping = AsyncMock(side_effect=RedisError("Connection refused"))

    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": "12345678"},
        headers=headers,
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


# --- Additional: invalid code format → 422 ---


async def test_verify_email_invalid_code_format_returns_422(app_client, fake_redis):
    """POST /auth/verify-email with non-6-digit code → 422."""
    _data, headers = await _register_and_get_auth(app_client, email="fmt@example.com")

    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": "abc"},
        headers=headers,
    )

    assert response.status_code == 422


# --- Additional: code is consumed after successful verification ---


async def test_verify_email_code_consumed_after_success(app_client, fake_redis):
    """After successful verification, the code is deleted from Redis."""
    data, headers = await _register_and_get_auth(app_client, email="consumed@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    # Verify email
    response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )
    assert response.status_code == 200

    # Code should be gone from Redis
    stored = await fake_redis.get(f"verify_code:{data['user_id']}")
    assert stored is None
