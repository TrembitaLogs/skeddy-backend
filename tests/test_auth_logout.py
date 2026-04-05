from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import jwt as pyjwt
from redis.exceptions import RedisError

from app.config import settings
from app.models.user import User
from app.services.auth_service import hash_password

LOGOUT_URL = "/api/v1/auth/logout"
REGISTER_URL = "/api/v1/auth/register"
REFRESH_URL = "/api/v1/auth/refresh"
ME_URL = "/api/v1/auth/me"

_TEST_PASSWORD = "securePass1"


async def _register_and_get_tokens(app_client, email="logout@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


def _create_expired_token(user_id) -> str:
    """Create a JWT that is already expired."""
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
        "exp": now - timedelta(hours=1),
        "iat": now - timedelta(hours=25),
    }
    return pyjwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


# --- POST /auth/logout ---


async def test_logout_with_valid_jwt_returns_200_and_deletes_tokens(app_client):
    """POST /auth/logout with valid JWT -> 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client)

    response = await app_client.post(
        LOGOUT_URL,
        headers={"Authorization": f"Bearer {reg['access_token']}"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_logout_invalidates_refresh_tokens(app_client):
    """After logout, old refresh token must be invalid."""
    reg = await _register_and_get_tokens(app_client, email="logout-inv@example.com")

    # Logout
    response = await app_client.post(
        LOGOUT_URL,
        headers={"Authorization": f"Bearer {reg['access_token']}"},
    )
    assert response.status_code == 200

    # Try to refresh with the old token — should fail
    refresh_response = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": reg["refresh_token"]},
    )
    assert refresh_response.status_code == 401


async def test_logout_without_jwt_returns_401(app_client):
    """POST /auth/logout without Authorization header -> 401."""
    response = await app_client.post(LOGOUT_URL)

    assert response.status_code == 401


# --- get_current_user dependency (tested through /auth/logout) ---


async def test_logout_with_expired_jwt_returns_401(app_client, db_session):
    """POST /auth/logout with expired JWT -> 401 INVALID_OR_EXPIRED_TOKEN."""
    user = User(email="expired@example.com", password_hash=hash_password(_TEST_PASSWORD))
    db_session.add(user)
    await db_session.flush()

    expired_token = _create_expired_token(user.id)

    response = await app_client.post(
        LOGOUT_URL,
        headers={"Authorization": f"Bearer {expired_token}"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_OR_EXPIRED_TOKEN"


async def test_logout_with_deleted_user_jwt_returns_401(app_client, db_session):
    """POST /auth/logout with JWT of deleted user -> 401 USER_NOT_FOUND."""
    # Register user and get a valid JWT
    reg = await _register_and_get_tokens(app_client, email="deleted@example.com")

    # Delete the user directly from DB
    user = await db_session.get(User, reg["user_id"])
    await db_session.delete(user)
    await db_session.commit()

    # Try to logout with the now-orphaned JWT
    response = await app_client.post(
        LOGOUT_URL,
        headers={"Authorization": f"Bearer {reg['access_token']}"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "USER_NOT_FOUND"


# --- Access token blacklist (SKE-23) ---


async def test_logout_blacklists_access_token(app_client, fake_redis):
    """After logout, the access token JTI is added to the Redis blacklist."""
    reg = await _register_and_get_tokens(app_client, email="blacklist@example.com")
    token = reg["access_token"]

    # Decode to get the JTI
    payload = pyjwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    jti = payload["jti"]

    # Logout
    resp = await app_client.post(LOGOUT_URL, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200

    # JTI should be in the fake_redis store
    assert fake_redis._store.get(f"blacklist:{jti}") == "1"


async def test_blacklisted_token_rejected_by_get_current_user(app_client, fake_redis):
    """A blacklisted access token must be rejected with 401 TOKEN_REVOKED."""
    reg = await _register_and_get_tokens(app_client, email="revoked@example.com")
    token = reg["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Confirm the token works before blacklisting
    me_resp = await app_client.get(ME_URL, headers=headers)
    assert me_resp.status_code == 200

    # Logout (blacklists the token)
    logout_resp = await app_client.post(LOGOUT_URL, headers=headers)
    assert logout_resp.status_code == 200

    # Token should now be rejected
    me_resp2 = await app_client.get(ME_URL, headers=headers)
    assert me_resp2.status_code == 401
    assert me_resp2.json()["error"]["code"] == "TOKEN_REVOKED"


async def test_non_blacklisted_token_still_works(app_client):
    """A valid, non-blacklisted access token must still work normally."""
    reg = await _register_and_get_tokens(app_client, email="valid@example.com")
    headers = {"Authorization": f"Bearer {reg['access_token']}"}

    me_resp = await app_client.get(ME_URL, headers=headers)
    assert me_resp.status_code == 200
    assert me_resp.json()["user_id"] == reg["user_id"]


async def test_blacklist_entry_has_ttl(app_client, fake_redis):
    """The blacklist Redis key should be set via setex (with TTL)."""
    reg = await _register_and_get_tokens(app_client, email="ttl@example.com")
    token = reg["access_token"]

    await app_client.post(LOGOUT_URL, headers={"Authorization": f"Bearer {token}"})

    # Verify setex was called with a positive TTL for the blacklist key
    blacklist_calls = [
        call
        for call in fake_redis.setex.call_args_list
        if str(call[0][0]).startswith("blacklist:")
    ]
    assert len(blacklist_calls) == 1
    _key, ttl, _value = blacklist_calls[0][0]
    assert ttl > 0
    assert ttl <= settings.JWT_ACCESS_TOKEN_EXPIRE_HOURS * 3600


async def test_redis_unavailable_on_blacklist_check_returns_503(
    app_client, fake_redis, db_session
):
    """When Redis is down during blacklist check, fail-closed with 503."""
    reg = await _register_and_get_tokens(app_client, email="redis-down@example.com")
    token = reg["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # Make Redis exists() raise an error (simulating outage)
    fake_redis.exists = AsyncMock(side_effect=RedisError("connection refused"))

    me_resp = await app_client.get(ME_URL, headers=headers)
    assert me_resp.status_code == 503


async def test_token_without_jti_still_works(app_client, db_session):
    """Tokens issued before the JTI feature (no jti claim) should still work."""
    user = User(email="no-jti@example.com", password_hash=hash_password(_TEST_PASSWORD))
    db_session.add(user)
    await db_session.flush()

    # Create a token without JTI (legacy format)
    now = datetime.now(UTC)
    payload = {
        "sub": str(user.id),
        "exp": now + timedelta(hours=24),
        "iat": now,
    }
    legacy_token = pyjwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

    me_resp = await app_client.get(ME_URL, headers={"Authorization": f"Bearer {legacy_token}"})
    assert me_resp.status_code == 200
