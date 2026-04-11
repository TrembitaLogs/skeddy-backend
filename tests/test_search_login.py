from uuid import UUID

from sqlalchemy import select

from app.models.paired_device import PairedDevice

SEARCH_LOGIN_URL = "/api/v1/auth/search-login"
REGISTER_URL = "/api/v1/auth/register"

_TEST_PASSWORD = "securePass1"


# --- Helpers ---


async def _register(client, email="searchlogin@example.com"):
    """Register a user and return (user_id,)."""
    resp = await client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 201
    return resp.json()["user_id"]


def _login_body(email="searchlogin@example.com", device_id="android-dev-001", **overrides):
    body = {
        "email": email,
        "password": _TEST_PASSWORD,
        "device_id": device_id,
        "timezone": "America/New_York",
    }
    body.update(overrides)
    return body


# --- Test 1: POST /auth/search-login with valid credentials → 200, device_token ---


async def test_search_login_valid_credentials_returns_device_token(app_client):
    """POST /auth/search-login with valid email/password → 200 with device_token and user_id."""
    email = "login-ok@example.com"
    user_id = await _register(app_client, email)

    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email),
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "device_token" in data
    assert isinstance(data["device_token"], str)
    assert len(data["device_token"]) > 0
    assert data["user_id"] == user_id


# --- Test 2: POST /auth/search-login with invalid password → 401 ---


async def test_search_login_invalid_password_returns_401(app_client):
    """POST /auth/search-login with wrong password → 401 INVALID_CREDENTIALS."""
    email = "login-bad-pw@example.com"
    await _register(app_client, email)

    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, password="wrongPassword123"),
    )

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "INVALID_CREDENTIALS"


# --- Test 3: POST /auth/search-login with non-existent email → 401 ---


async def test_search_login_unknown_email_returns_401(app_client):
    """POST /auth/search-login with non-existent email → 401 INVALID_CREDENTIALS."""
    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email="ghost@example.com"),
    )

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "INVALID_CREDENTIALS"


# --- Test 4: POST /auth/search-login with invalid timezone → 422 ---


async def test_search_login_invalid_timezone_returns_422(app_client):
    """POST /auth/search-login with invalid IANA timezone → 422 INVALID_TIMEZONE."""
    email = "login-badtz@example.com"
    await _register(app_client, email)

    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, timezone="Not/A/Timezone"),
    )

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INVALID_TIMEZONE"


# --- Test 5: device_model stored when provided ---


async def test_search_login_stores_device_model(app_client, db_session):
    """POST /auth/search-login with device_model → device_model persisted in DB."""
    email = "login-model@example.com"
    user_id = await _register(app_client, email)

    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, device_model="Google Pixel 8 Pro"),
    )
    assert resp.status_code == 200

    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    device = result.scalar_one()
    assert device.device_model == "Google Pixel 8 Pro"


# --- Test 6: device_model is None when not provided ---


async def test_search_login_without_device_model_stores_none(app_client, db_session):
    """POST /auth/search-login without device_model → device_model is None."""
    email = "login-nomodel@example.com"
    user_id = await _register(app_client, email)

    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, device_id="nomodel-dev"),
    )
    assert resp.status_code == 200

    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    device = result.scalar_one()
    assert device.device_model is None


# --- Test 7: Login on new device replaces old device ---


async def test_search_login_new_device_replaces_old(app_client, db_session):
    """Re-login with new device_id removes the old PairedDevice record."""
    email = "login-replace@example.com"
    user_id = await _register(app_client, email)

    # First login
    resp1 = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, device_id="old-device"),
    )
    assert resp1.status_code == 200

    # Second login with different device
    resp2 = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, device_id="new-device", timezone="US/Pacific"),
    )
    assert resp2.status_code == 200

    # Only the new device should exist
    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    devices = result.scalars().all()
    assert len(devices) == 1
    assert devices[0].device_id == "new-device"
    assert devices[0].timezone == "US/Pacific"


# --- Test 8: Same device_id registered to another user gets replaced ---


async def test_search_login_device_registered_to_other_user_replaced(app_client, db_session):
    """If device_id is already registered to another user, old record is deleted."""
    email_a = "login-usera@example.com"
    email_b = "login-userb@example.com"
    user_id_a = await _register(app_client, email_a)
    user_id_b = await _register(app_client, email_b)

    # UserA logs in with shared device
    resp_a = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email_a, device_id="shared-device"),
    )
    assert resp_a.status_code == 200

    # UserB logs in with same device
    resp_b = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email_b, device_id="shared-device"),
    )
    assert resp_b.status_code == 200

    # UserA should have no device
    result_a = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id_a))
    )
    assert result_a.scalar_one_or_none() is None

    # UserB should have the device
    result_b = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id_b))
    )
    device = result_b.scalar_one()
    assert device.device_id == "shared-device"


# --- Test 9: Same device re-login generates new token (old token becomes invalid) ---


async def test_search_login_same_device_generates_new_token(app_client):
    """Re-login with same device_id generates a new device_token."""
    email = "login-retoken@example.com"
    await _register(app_client, email)

    resp1 = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, device_id="same-device"),
    )
    assert resp1.status_code == 200
    token1 = resp1.json()["device_token"]

    resp2 = await app_client.post(
        SEARCH_LOGIN_URL,
        json=_login_body(email=email, device_id="same-device"),
    )
    assert resp2.status_code == 200
    token2 = resp2.json()["device_token"]

    # New token should be different
    assert token1 != token2


# --- Test 10: Old pairing endpoints removed ---


async def test_old_pairing_endpoints_return_404(app_client):
    """Old pairing endpoints should return 404/405 since they are removed."""
    resp_generate = await app_client.post("/api/v1/pairing/generate")
    assert resp_generate.status_code in (404, 405)

    resp_confirm = await app_client.post(
        "/api/v1/pairing/confirm",
        json={"code": "123456", "device_id": "dev-001", "timezone": "America/New_York"},
    )
    assert resp_confirm.status_code in (404, 405)

    resp_status = await app_client.get("/api/v1/pairing/status")
    assert resp_status.status_code in (404, 405)

    resp_unpair = await app_client.delete("/api/v1/pairing")
    assert resp_unpair.status_code in (404, 405)
