import uuid
from unittest.mock import AsyncMock

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.services.fcm_service import update_user_fcm_token

FCM_REGISTER_URL = "/api/v1/fcm/register"
REGISTER_URL = "/api/v1/auth/register"

_TEST_PASSWORD = "securePass1"


# --- Helper ---


async def _register_and_get_token(app_client, email="fcm@example.com"):
    """Register a user and return access_token."""
    reg = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert reg.status_code == 201
    return reg.json()["access_token"]


# --- POST /fcm/register: Success ---


async def test_register_fcm_token_returns_200(app_client):
    """POST /fcm/register with valid token and auth -> 200 {ok: true}."""
    token = await _register_and_get_token(app_client)

    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "valid_fcm_token_string"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- POST /fcm/register: Auth errors ---


async def test_register_fcm_token_without_auth_returns_401(app_client):
    """POST /fcm/register without Authorization header -> 401."""
    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "some_token"},
    )

    assert response.status_code == 401


async def test_register_fcm_token_with_invalid_jwt_returns_401(app_client):
    """POST /fcm/register with invalid JWT -> 401."""
    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "some_token"},
        headers={"Authorization": "Bearer invalid-jwt-token"},
    )

    assert response.status_code == 401


# --- POST /fcm/register: Validation errors ---


async def test_register_fcm_token_empty_string_returns_422(app_client):
    """POST /fcm/register with empty fcm_token -> 422."""
    token = await _register_and_get_token(app_client)

    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": ""},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 422


async def test_register_fcm_token_missing_field_returns_422(app_client):
    """POST /fcm/register without fcm_token field -> 422."""
    token = await _register_and_get_token(app_client)

    response = await app_client.post(
        FCM_REGISTER_URL,
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 422


async def test_register_fcm_token_too_long_returns_422(app_client):
    """POST /fcm/register with fcm_token exceeding 500 chars -> 422."""
    token = await _register_and_get_token(app_client)

    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "x" * 501},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 422


# --- POST /fcm/register: DB integration ---


async def test_fcm_token_saved_in_database(app_client, db_session):
    """Verify fcm_token is persisted in the users table."""
    token = await _register_and_get_token(app_client)

    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "my_device_fcm_token_123"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200

    result = await db_session.execute(select(User).where(User.email == "fcm@example.com"))
    user = result.scalar_one()
    assert user.fcm_token == "my_device_fcm_token_123"


async def test_fcm_token_updated_on_second_registration(app_client, db_session):
    """Verify fcm_token is overwritten when registering a new token."""
    token = await _register_and_get_token(app_client, email="update@example.com")

    # First registration
    await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "first_token"},
        headers={"Authorization": f"Bearer {token}"},
    )

    # Second registration with new token
    response = await app_client.post(
        FCM_REGISTER_URL,
        json={"fcm_token": "second_token"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200

    result = await db_session.execute(select(User).where(User.email == "update@example.com"))
    user = result.scalar_one()
    assert user.fcm_token == "second_token"


# --- update_user_fcm_token service unit test ---


async def test_update_user_fcm_token_calls_db():
    """Verify update_user_fcm_token executes and commits."""
    mock_db = AsyncMock(spec=AsyncSession)
    user_id = uuid.uuid4()

    await update_user_fcm_token(mock_db, user_id, "new_token")

    mock_db.execute.assert_called_once()
    mock_db.commit.assert_called_once()
