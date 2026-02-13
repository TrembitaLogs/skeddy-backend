from datetime import UTC, datetime, timedelta

import jwt as pyjwt

from app.config import settings
from app.models.user import User
from app.services.auth_service import hash_password

LOGOUT_URL = "/api/v1/auth/logout"
REGISTER_URL = "/api/v1/auth/register"
REFRESH_URL = "/api/v1/auth/refresh"

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
