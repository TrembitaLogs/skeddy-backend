PROFILE_URL = "/api/v1/profile"
REGISTER_URL = "/api/v1/auth/register"
ME_URL = "/api/v1/auth/me"

_TEST_PASSWORD = "securePass1"


async def _register_and_get_tokens(app_client, email="phone@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


# --- Test Strategy 1: PATCH /profile with valid E.164 number → 200 ---


async def test_update_phone_valid_e164_returns_200(app_client):
    """PATCH /profile with valid E.164 number → 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="phone1@example.com")

    response = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": "+12025551234"},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True


# --- Test Strategy 2: PATCH /profile with another user's phone → 409 ---


async def test_update_phone_taken_by_another_user_returns_409(app_client):
    """PATCH /profile with phone belonging to another user → 409 PHONE_ALREADY_EXISTS."""
    reg1 = await _register_and_get_tokens(app_client, email="phone2a@example.com")
    reg2 = await _register_and_get_tokens(app_client, email="phone2b@example.com")

    # User 1 sets phone
    resp1 = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": "+380501234567"},
        headers=_auth_header(reg1["access_token"]),
    )
    assert resp1.status_code == 200

    # User 2 tries the same phone
    response = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": "+380501234567"},
        headers=_auth_header(reg2["access_token"]),
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PHONE_ALREADY_EXISTS"


# --- Test Strategy 3: PATCH /profile with phone_number=null → 200, phone removed ---


async def test_update_phone_null_removes_phone(app_client):
    """PATCH /profile with phone_number=null → 200, phone removed."""
    reg = await _register_and_get_tokens(app_client, email="phone3@example.com")

    # Set phone first
    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": "+12025551234"},
        headers=_auth_header(reg["access_token"]),
    )
    assert resp.status_code == 200

    # Remove phone
    response = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": None},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True

    # Verify via /me
    me = await app_client.get(ME_URL, headers=_auth_header(reg["access_token"]))
    assert me.json()["phone_number"] is None


# --- Test Strategy 4: PATCH /profile with same number already set → 200 (idempotent) ---


async def test_update_phone_same_number_is_idempotent(app_client):
    """PATCH /profile with the same number user already has → 200 (idempotent)."""
    reg = await _register_and_get_tokens(app_client, email="phone4@example.com")
    phone = "+12025559999"

    # Set phone
    resp = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": phone},
        headers=_auth_header(reg["access_token"]),
    )
    assert resp.status_code == 200

    # Set the same phone again
    response = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": phone},
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True


# --- Test Strategy 5: GET /auth/me after update shows new phone ---


async def test_get_profile_shows_updated_phone(app_client):
    """GET /auth/me after PATCH /profile shows updated phone_number."""
    reg = await _register_and_get_tokens(app_client, email="phone5@example.com")
    phone = "+380509876543"

    await app_client.patch(
        PROFILE_URL,
        json={"phone_number": phone},
        headers=_auth_header(reg["access_token"]),
    )

    me = await app_client.get(ME_URL, headers=_auth_header(reg["access_token"]))

    assert me.status_code == 200
    assert me.json()["phone_number"] == phone


# --- Additional: no auth → 401 ---


async def test_update_profile_without_jwt_returns_401(app_client):
    """PATCH /profile without Authorization header → 401."""
    response = await app_client.patch(
        PROFILE_URL,
        json={"phone_number": "+12025551234"},
    )

    assert response.status_code == 401
