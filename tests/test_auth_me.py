import hashlib
import json
from uuid import UUID

from sqlalchemy import select

from app.models.user import User

ME_URL = "/api/v1/auth/me"
REGISTER_URL = "/api/v1/auth/register"
VERIFY_EMAIL_URL = "/api/v1/auth/verify-email"

_TEST_PASSWORD = "securePass1"
_VERIFY_CODE = "593817"


async def _register_and_get_auth(app_client, email):
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


# --- GET /auth/me ---


async def test_get_profile_without_token_returns_401(app_client):
    """GET /auth/me without Authorization header -> 401."""
    response = await app_client.get(ME_URL)

    assert response.status_code == 401


async def test_get_profile_with_invalid_token_returns_401(app_client):
    """GET /auth/me with invalid JWT -> 401."""
    response = await app_client.get(
        ME_URL,
        headers={"Authorization": "Bearer invalid-token"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_OR_EXPIRED_TOKEN"


async def test_get_profile_with_valid_token_returns_profile(app_client):
    """GET /auth/me with valid JWT -> 200 with user_id, email, phone_number, created_at."""
    reg = await app_client.post(
        REGISTER_URL,
        json={"email": "profile@example.com", "password": _TEST_PASSWORD},
    )
    assert reg.status_code == 201
    reg_data = reg.json()

    response = await app_client.get(
        ME_URL,
        headers={"Authorization": f"Bearer {reg_data['access_token']}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == reg_data["user_id"]
    assert data["email"] == "profile@example.com"
    assert "email_verified" in data
    assert "phone_number" in data
    assert "created_at" in data


async def test_get_profile_phone_number_is_null_for_new_user(app_client):
    """GET /auth/me -> phone_number is null for newly registered user."""
    reg = await app_client.post(
        REGISTER_URL,
        json={"email": "nophone@example.com", "password": _TEST_PASSWORD},
    )
    assert reg.status_code == 201
    reg_data = reg.json()

    response = await app_client.get(
        ME_URL,
        headers={"Authorization": f"Bearer {reg_data['access_token']}"},
    )

    assert response.status_code == 200
    assert response.json()["phone_number"] is None


# --- Test Strategy (task 23): email_verified in GET /auth/me ---


async def test_get_profile_new_registration_email_verified_false(app_client):
    """TS#2: New registration -> GET /auth/me -> email_verified: false."""
    _data, headers = await _register_and_get_auth(app_client, "new-reg-me@example.com")

    response = await app_client.get(ME_URL, headers=headers)

    assert response.status_code == 200
    assert response.json()["email_verified"] is False


async def test_get_profile_existing_user_email_verified_true(app_client, db_session):
    """TS#1: Existing (migrated) user -> GET /auth/me -> email_verified: true."""
    data, headers = await _register_and_get_auth(app_client, "existing-me@example.com")

    # Simulate existing user (DB migration sets email_verified=true)
    result = await db_session.execute(select(User).where(User.id == UUID(data["user_id"])))
    user = result.scalar_one()
    user.email_verified = True
    await db_session.flush()

    response = await app_client.get(ME_URL, headers=headers)

    assert response.status_code == 200
    assert response.json()["email_verified"] is True


async def test_get_profile_after_verify_email_returns_verified_true(app_client, fake_redis):
    """TS#3: POST /auth/verify-email -> GET /auth/me -> email_verified: true."""
    data, headers = await _register_and_get_auth(app_client, "verify-me@example.com")
    await _store_verify_code_in_redis(fake_redis, data["user_id"])

    # Verify email
    verify_resp = await app_client.post(
        VERIFY_EMAIL_URL,
        json={"code": _VERIFY_CODE},
        headers=headers,
    )
    assert verify_resp.status_code == 200

    # GET /me should now return email_verified: true
    me_resp = await app_client.get(ME_URL, headers=headers)

    assert me_resp.status_code == 200
    assert me_resp.json()["email_verified"] is True


async def test_get_profile_response_matches_api_contract(app_client):
    """TS#4: Response schema matches API contract."""
    _data, headers = await _register_and_get_auth(app_client, "contract-me@example.com")

    response = await app_client.get(ME_URL, headers=headers)

    assert response.status_code == 200
    body = response.json()

    # Verify all fields from API contract are present and no extra fields
    assert set(body.keys()) == {
        "user_id",
        "email",
        "email_verified",
        "phone_number",
        "created_at",
    }

    # Verify field types
    assert isinstance(body["user_id"], str)
    assert isinstance(body["email"], str)
    assert isinstance(body["email_verified"], bool)
    assert body["phone_number"] is None or isinstance(body["phone_number"], str)
    assert isinstance(body["created_at"], str)
