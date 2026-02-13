ME_URL = "/api/v1/auth/me"
REGISTER_URL = "/api/v1/auth/register"

_TEST_PASSWORD = "securePass1"


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
