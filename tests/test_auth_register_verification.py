"""Tests for verification code sending during POST /auth/register.

Test strategy from task 20:
1. POST /auth/register → 201 with tokens
2. Verify Redis: verify_code:{user_id} exists
3. Verify that email was sent with 6-digit code
4. GET /auth/me → email_verified: false
5. POST /auth/verify-email with code from email → 200
6. GET /auth/me → email_verified: true
"""

import hashlib
import json
from unittest.mock import AsyncMock, patch

REGISTER_URL = "/api/v1/auth/register"
VERIFY_EMAIL_URL = "/api/v1/auth/verify-email"
ME_URL = "/api/v1/auth/me"

_TEST_PASSWORD = "securePass1"


# --- Test Strategy: 1. POST /auth/register → 201 with tokens ---


@patch("app.routers.auth.send_verification_code", new_callable=AsyncMock)
async def test_register_returns_201_with_tokens(mock_send, app_client):
    """POST /auth/register → 201 with user_id, access_token, refresh_token."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "reg-verify@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    data = response.json()
    assert "user_id" in data
    assert "access_token" in data
    assert "refresh_token" in data


# --- Test Strategy: 2. Verify Redis: verify_code:{user_id} exists ---


@patch("app.routers.auth.send_verification_code", new_callable=AsyncMock)
async def test_register_stores_verification_code_in_redis(mock_send, app_client, fake_redis):
    """POST /auth/register stores SHA256 hashed verification code in Redis."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "redis-verify@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    user_id = response.json()["user_id"]

    # Verify code stored in Redis under verify_code:{user_id}
    stored = await fake_redis.get(f"verify_code:{user_id}")
    assert stored is not None

    data = json.loads(stored)
    assert "code_hash" in data
    assert data["attempts"] == 0

    # Verify the hash matches the code sent via email
    plain_code = mock_send.call_args[0][1]
    expected_hash = hashlib.sha256(plain_code.encode()).hexdigest()
    assert data["code_hash"] == expected_hash


@patch("app.routers.auth.send_verification_code", new_callable=AsyncMock)
async def test_register_stores_code_with_30min_ttl(mock_send, app_client, fake_redis):
    """Verification code is stored with 1800s (30 min) TTL."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "ttl-verify@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    user_id = response.json()["user_id"]

    # Verify setex was called with correct TTL (1800 seconds)
    setex_calls = fake_redis.setex.call_args_list
    code_setex = [c for c in setex_calls if f"verify_code:{user_id}" in str(c)]
    assert len(code_setex) == 1
    assert code_setex[0][0][1] == 1800


# --- Test Strategy: 3. Verify that email was sent with 6-digit code ---


@patch("app.routers.auth.send_verification_code", new_callable=AsyncMock)
async def test_register_sends_verification_email_with_code(mock_send, app_client):
    """POST /auth/register sends verification email with 6-digit code."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "email-check@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    mock_send.assert_awaited_once()

    call_args = mock_send.call_args
    assert call_args[0][0] == "email-check@example.com"

    code = call_args[0][1]
    assert len(code) == 8
    assert code.isdigit()
    assert 10000000 <= int(code) <= 99999999


# --- Test Strategy: 4. GET /auth/me → email_verified: false ---


@patch("app.routers.auth.send_verification_code", new_callable=AsyncMock)
async def test_register_user_is_not_verified(mock_send, app_client):
    """After registration, GET /auth/me returns email_verified: false."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "unverified-me@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    headers = {"Authorization": f"Bearer {response.json()['access_token']}"}

    me_response = await app_client.get(ME_URL, headers=headers)
    assert me_response.status_code == 200
    assert me_response.json()["email_verified"] is False


# --- Test Strategy: 5 & 6. Full flow: register → verify-email → email_verified: true ---


@patch("app.routers.auth.send_verification_code", new_callable=AsyncMock)
async def test_register_then_verify_email_full_flow(mock_send, app_client, fake_redis):
    """Full flow: register → get code → verify-email → email_verified: true."""
    # Step 1: Register
    reg_response = await app_client.post(
        REGISTER_URL,
        json={"email": "full-flow@example.com", "password": _TEST_PASSWORD},
    )

    assert reg_response.status_code == 201
    data = reg_response.json()
    headers = {"Authorization": f"Bearer {data['access_token']}"}

    # Step 2: Get the code that was sent via email
    code = mock_send.call_args[0][1]

    # Step 3: Verify email with the code
    verify_response = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": code},
        headers=headers,
    )

    assert verify_response.status_code == 200
    assert verify_response.json() == {"ok": True}

    # Step 4: GET /auth/me → email_verified: true
    me_response = await app_client.get(ME_URL, headers=headers)
    assert me_response.status_code == 200
    assert me_response.json()["email_verified"] is True


# --- Additional: SMTP failure does not break registration ---


@patch(
    "app.routers.auth.send_verification_code",
    new_callable=AsyncMock,
    side_effect=OSError("SMTP connection failed"),
)
async def test_register_smtp_failure_still_returns_201(mock_send, app_client):
    """Registration succeeds even when verification email fails to send."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "smtp-fail@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    data = response.json()
    assert "user_id" in data
    assert "access_token" in data
    assert "refresh_token" in data
    mock_send.assert_awaited_once()


# --- Additional: SMTP failure — code still stored in Redis (store happens before send) ---


@patch(
    "app.routers.auth.send_verification_code",
    new_callable=AsyncMock,
    side_effect=OSError("SMTP connection failed"),
)
async def test_register_smtp_failure_code_still_in_redis(mock_send, app_client, fake_redis):
    """When SMTP fails, the verification code is still stored in Redis."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": "smtp-redis@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 201
    user_id = response.json()["user_id"]

    # Code should still be in Redis (store_verify_code runs before send_verification_code)
    stored = await fake_redis.get(f"verify_code:{user_id}")
    assert stored is not None
