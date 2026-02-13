CHANGE_PASSWORD_URL = "/api/v1/auth/change-password"
REGISTER_URL = "/api/v1/auth/register"
LOGIN_URL = "/api/v1/auth/login"
REFRESH_URL = "/api/v1/auth/refresh"

_TEST_PASSWORD = "securePass1"
_NEW_PASSWORD = "newSecurePass2"


async def _register_and_get_tokens(app_client, email="chpwd@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


# --- Test Strategy: 1. Wrong current_password → 401 INVALID_CURRENT_PASSWORD ---


async def test_change_password_wrong_current_password_returns_401(app_client):
    """POST /auth/change-password with wrong current_password → 401."""
    reg = await _register_and_get_tokens(app_client, email="chpwd-wrong@example.com")

    response = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={
            "current_password": "wrongPassword123",
            "new_password": _NEW_PASSWORD,
        },
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "INVALID_CURRENT_PASSWORD"


# --- Test Strategy: 2. new_password < 8 chars → 422 Validation Error ---


async def test_change_password_short_new_password_returns_422(app_client):
    """POST /auth/change-password with new_password < 8 chars → 422."""
    reg = await _register_and_get_tokens(app_client, email="chpwd-short@example.com")

    response = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={
            "current_password": _TEST_PASSWORD,
            "new_password": "short",
        },
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 422


# --- Test Strategy: 3. Valid data → 200, password changed ---


async def test_change_password_valid_data_returns_200(app_client):
    """POST /auth/change-password with valid data → 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="chpwd-ok@example.com")

    response = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={
            "current_password": _TEST_PASSWORD,
            "new_password": _NEW_PASSWORD,
        },
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy: 4. After password change, old refresh token → 401 ---


async def test_change_password_invalidates_refresh_tokens(app_client):
    """After change-password, old refresh token must be invalid."""
    reg = await _register_and_get_tokens(app_client, email="chpwd-rt@example.com")

    # Change password
    response = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={
            "current_password": _TEST_PASSWORD,
            "new_password": _NEW_PASSWORD,
        },
        headers=_auth_header(reg["access_token"]),
    )
    assert response.status_code == 200

    # Try to refresh with old token — should fail
    refresh_response = await app_client.post(
        REFRESH_URL,
        json={"refresh_token": reg["refresh_token"]},
    )
    assert refresh_response.status_code == 401


# --- Test Strategy: 5. After password change, can log in with new password ---


async def test_change_password_allows_login_with_new_password(app_client):
    """After change-password, login with new password should succeed."""
    email = "chpwd-login@example.com"
    reg = await _register_and_get_tokens(app_client, email=email)

    # Change password
    response = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={
            "current_password": _TEST_PASSWORD,
            "new_password": _NEW_PASSWORD,
        },
        headers=_auth_header(reg["access_token"]),
    )
    assert response.status_code == 200

    # Login with new password
    login_response = await app_client.post(
        LOGIN_URL,
        json={"email": email, "password": _NEW_PASSWORD},
    )
    assert login_response.status_code == 200
    assert "access_token" in login_response.json()


# --- Additional: no auth → 401 ---


async def test_change_password_without_jwt_returns_401(app_client):
    """POST /auth/change-password without Authorization header → 401."""
    response = await app_client.post(
        CHANGE_PASSWORD_URL,
        json={
            "current_password": _TEST_PASSWORD,
            "new_password": _NEW_PASSWORD,
        },
    )

    assert response.status_code == 401
