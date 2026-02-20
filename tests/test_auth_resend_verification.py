"""Tests for POST /auth/resend-verification.

Test strategy from task 21:
1. POST /auth/resend-verification (email_verified=false) → 200 OK
2. Verify Redis: verify_code:{user_id} with new code_hash
3. Verify email sent with new code
4. POST /auth/resend-verification (email_verified=true) → 400 ALREADY_VERIFIED
5. 4th request per hour → 429 RATE_LIMIT_EXCEEDED
6. POST without JWT → 401 Unauthorized
"""

import hashlib
import json
from unittest.mock import AsyncMock, patch

from redis.exceptions import RedisError

from app.middleware.rate_limiter import limiter

REGISTER_URL = "/api/v1/auth/register"
RESEND_VERIFICATION_URL = "/api/v1/auth/resend-verification"
VERIFY_EMAIL_URL = "/api/v1/auth/verify-email"

_TEST_PASSWORD = "securePass1"
_TEST_EMAIL = "resend@example.com"
_VERIFY_CODE = "593817"


async def _register_and_get_auth(app_client, email=_TEST_EMAIL):
    """Helper: register a user and return (response_data, auth_headers)."""
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


# --- Test Strategy 1 & 2: POST → 200 OK, new code in Redis ---


async def test_resend_verification_returns_200_and_stores_new_code(app_client, fake_redis):
    """POST /auth/resend-verification (unverified) → 200, new code stored in Redis."""
    data, headers = await _register_and_get_auth(app_client)
    user_id = data["user_id"]

    # Clear any code stored during registration
    await fake_redis.delete(f"verify_code:{user_id}")

    with patch("app.routers.auth.send_verification_code", new_callable=AsyncMock):
        response = await app_client.post(RESEND_VERIFICATION_URL, headers=headers)

    assert response.status_code == 200
    assert response.json() == {"ok": True}

    # Verify new code stored in Redis with fresh attempts counter
    stored = await fake_redis.get(f"verify_code:{user_id}")
    assert stored is not None
    code_data = json.loads(stored)
    assert "code_hash" in code_data
    assert code_data["attempts"] == 0


# --- Test Strategy 3: Verify email sent with new code ---


async def test_resend_verification_sends_email_with_code(app_client, fake_redis):
    """Resend verification triggers email with a 6-digit code to the user's address."""
    _data, headers = await _register_and_get_auth(app_client, email="resend-email@example.com")

    with patch("app.routers.auth.send_verification_code", new_callable=AsyncMock) as mock_send:
        response = await app_client.post(RESEND_VERIFICATION_URL, headers=headers)

    assert response.status_code == 200
    mock_send.assert_called_once()
    to_email, code = mock_send.call_args[0]
    assert to_email == "resend-email@example.com"
    assert len(code) == 6
    assert code.isdigit()


# --- Test Strategy 4: Already verified → 400 ALREADY_VERIFIED ---


async def test_resend_verification_already_verified_returns_400(app_client, fake_redis):
    """POST /auth/resend-verification when email_verified=true → 400 ALREADY_VERIFIED."""
    data, headers = await _register_and_get_auth(app_client, email="already-v@example.com")

    # Verify email first
    await _store_verify_code_in_redis(fake_redis, data["user_id"])
    verify_resp = await app_client.post(
        VERIFY_EMAIL_URL, json={"code": _VERIFY_CODE}, headers=headers
    )
    assert verify_resp.status_code == 200

    # Attempt resend on verified account
    response = await app_client.post(RESEND_VERIFICATION_URL, headers=headers)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "ALREADY_VERIFIED"


# --- Test Strategy 5: 4th request per hour → 429 RATE_LIMIT_EXCEEDED ---


async def test_resend_verification_rate_limited_on_4th_request(app_client, fake_redis):
    """4th resend request within an hour → 429 RATE_LIMIT_EXCEEDED."""
    from limits.storage import MemoryStorage

    _data, headers = await _register_and_get_auth(app_client, email="ratelim@example.com")

    # Enable rate limiter with in-memory storage for this test
    original_storage = limiter._limiter.storage
    limiter._limiter.storage = MemoryStorage()
    limiter.enabled = True

    try:
        with patch("app.routers.auth.send_verification_code", new_callable=AsyncMock):
            # First 3 requests should succeed
            for i in range(3):
                resp = await app_client.post(RESEND_VERIFICATION_URL, headers=headers)
                assert resp.status_code == 200, f"Request {i + 1} should succeed"

            # 4th request should be rate limited
            resp = await app_client.post(RESEND_VERIFICATION_URL, headers=headers)
            assert resp.status_code == 429
            assert resp.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    finally:
        limiter._limiter.storage = original_storage
        limiter.enabled = False


# --- Test Strategy 6: No JWT → 401 Unauthorized ---


async def test_resend_verification_no_jwt_returns_401(app_client):
    """POST /auth/resend-verification without Authorization header → 401."""
    response = await app_client.post(RESEND_VERIFICATION_URL)

    assert response.status_code == 401


# --- Additional: Redis unavailable → 503 SERVICE_UNAVAILABLE ---


async def test_resend_verification_redis_unavailable_returns_503(app_client, fake_redis):
    """POST /auth/resend-verification when Redis is down → 503."""
    _data, headers = await _register_and_get_auth(app_client, email="redis-down@example.com")

    fake_redis.ping = AsyncMock(side_effect=RedisError("Connection refused"))

    response = await app_client.post(RESEND_VERIFICATION_URL, headers=headers)

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"
