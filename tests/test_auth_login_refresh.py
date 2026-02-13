from uuid import UUID

from app.models.user import User
from app.services.auth_service import (
    decode_access_token,
    hash_password,
    hash_refresh_token,
)

LOGIN_URL = "/api/v1/auth/login"
REGISTER_URL = "/api/v1/auth/register"
REFRESH_URL = "/api/v1/auth/refresh"

_TEST_PASSWORD = "securePass1"


async def _create_user(db_session, email="user@example.com"):
    """Helper: insert a user with a known password and return the model."""
    user = User(email=email, password_hash=hash_password(_TEST_PASSWORD))
    db_session.add(user)
    await db_session.flush()
    return user


# --- POST /auth/login ---


async def test_login_correct_password_returns_200_with_tokens(app_client, db_session):
    """POST /auth/login with correct password -> 200 with tokens."""
    user = await _create_user(db_session)

    response = await app_client.post(
        LOGIN_URL,
        json={"email": "user@example.com", "password": _TEST_PASSWORD},
    )

    assert response.status_code == 200
    data = response.json()
    assert UUID(data["user_id"]) == user.id
    assert "access_token" in data
    assert "refresh_token" in data

    # Verify access token is a valid JWT with correct user_id
    payload = decode_access_token(data["access_token"])
    assert payload is not None
    assert payload["sub"] == str(user.id)


async def test_login_wrong_password_returns_401(app_client, db_session):
    """POST /auth/login with wrong password -> 401 INVALID_CREDENTIALS."""
    await _create_user(db_session)

    response = await app_client.post(
        LOGIN_URL,
        json={"email": "user@example.com", "password": "wrongPassword1"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_CREDENTIALS"


async def test_login_nonexistent_email_returns_401(app_client):
    """POST /auth/login with non-existent email -> 401 INVALID_CREDENTIALS (not EMAIL_NOT_FOUND)."""
    response = await app_client.post(
        LOGIN_URL,
        json={"email": "nobody@example.com", "password": "somePassword1"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_CREDENTIALS"


# --- POST /auth/refresh ---


async def _register_user(app_client, email="refresh@example.com"):
    """Helper: register via API and return the response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


async def test_refresh_valid_token_returns_200_with_new_tokens(app_client):
    """POST /auth/refresh with valid token -> 200 with new tokens."""
    reg = await _register_user(app_client)
    old_refresh = reg["refresh_token"]

    response = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": old_refresh},
    )

    assert response.status_code == 200
    data = response.json()
    assert "user_id" in data
    assert data["user_id"] == reg["user_id"]
    assert "access_token" in data
    assert "refresh_token" in data
    # New tokens should differ from old ones
    assert data["refresh_token"] != old_refresh

    # Verify new access token is valid
    payload = decode_access_token(data["access_token"])
    assert payload is not None
    assert payload["sub"] == reg["user_id"]


async def test_refresh_concurrent_grace_period(app_client):
    """POST /auth/refresh with same token twice -> both succeed (grace period)."""
    reg = await _register_user(app_client, email="grace@example.com")
    old_refresh = reg["refresh_token"]

    # First refresh — rotates tokens, caches result in fake Redis
    response1 = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": old_refresh},
    )
    assert response1.status_code == 200
    data1 = response1.json()

    # Second refresh with same old token — returns cached result from Redis
    response2 = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": old_refresh},
    )
    assert response2.status_code == 200
    data2 = response2.json()

    # Both should return the exact same new token pair
    assert data1 == data2


async def test_refresh_token_reuse_after_grace_period_returns_401(app_client, fake_redis):
    """POST /auth/refresh with reused token after grace period -> 401."""
    reg = await _register_user(app_client, email="reuse@example.com")
    old_refresh = reg["refresh_token"]

    # First refresh succeeds — rotates tokens, caches result in Redis
    response1 = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": old_refresh},
    )
    assert response1.status_code == 200

    # Simulate grace period expiry by removing cached result from Redis
    old_hash = hash_refresh_token(old_refresh)
    fake_redis._store.pop(f"refresh_grace:{old_hash}", None)

    # Second refresh with same old token — token deleted from DB, cache expired
    response2 = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": old_refresh},
    )
    assert response2.status_code == 401
    assert response2.json()["error"]["code"] == "INVALID_REFRESH_TOKEN"
