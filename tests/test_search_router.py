from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select

from app.models.paired_device import PairedDevice

SEARCH_START_URL = "/api/v1/search/start"
SEARCH_STOP_URL = "/api/v1/search/stop"
SEARCH_STATUS_URL = "/api/v1/search/status"
DEVICE_OVERRIDE_URL = "/api/v1/search/device-override"
REGISTER_URL = "/api/v1/auth/register"
PAIRING_GENERATE_URL = "/api/v1/pairing/generate"
PAIRING_CONFIRM_URL = "/api/v1/pairing/confirm"

_TEST_PASSWORD = "securePass1"


async def _register_and_get_tokens(app_client, email="search@example.com"):
    """Helper: register a user via API and return response data with tokens."""
    response = await app_client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert response.status_code == 201
    return response.json()


async def _pair_device(app_client, access_token, device_id="test-device-001"):
    """Helper: pair a device via generate + confirm flow."""
    gen_resp = await app_client.post(
        PAIRING_GENERATE_URL,
        headers=_auth_header(access_token),
    )
    assert gen_resp.status_code == 201
    code = gen_resp.json()["code"]

    confirm_resp = await app_client.post(
        PAIRING_CONFIRM_URL,
        json={"code": code, "device_id": device_id, "timezone": "America/New_York"},
    )
    assert confirm_resp.status_code == 200
    return confirm_resp.json()


def _auth_header(access_token: str) -> dict:
    return {"Authorization": f"Bearer {access_token}"}


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


# --- Test Strategy 1: POST /search/start without JWT → 401 ---


async def test_start_search_without_jwt_returns_401(app_client):
    """POST /search/start without Authorization header -> 401."""
    response = await app_client.post(SEARCH_START_URL)
    assert response.status_code == 401


# --- POST /search/start without paired device → 400 ---


async def test_start_search_without_paired_device_returns_400(app_client):
    """POST /search/start with no paired device -> 400 NO_PAIRED_DEVICE."""
    reg = await _register_and_get_tokens(app_client, email="nopair@example.com")

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "NO_PAIRED_DEVICE"


# --- Test Strategy 2: POST /search/start → 200, {"ok": true} ---


async def test_start_search_returns_ok(app_client):
    """POST /search/start with paired device -> 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="start@example.com")
    await _pair_device(app_client, reg["access_token"])

    response = await app_client.post(
        SEARCH_START_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- Test Strategy 3: POST /search/start twice → 200 (idempotent) ---


async def test_start_search_twice_is_idempotent(app_client):
    """POST /search/start called twice -> both return 200."""
    reg = await _register_and_get_tokens(app_client, email="twice@example.com")
    await _pair_device(app_client, reg["access_token"])
    headers = _auth_header(reg["access_token"])

    resp1 = await app_client.post(SEARCH_START_URL, headers=headers)
    resp2 = await app_client.post(SEARCH_START_URL, headers=headers)

    assert resp1.status_code == 200
    assert resp2.status_code == 200


# --- Test Strategy 4: POST /search/stop → 200, {"ok": true} ---


async def test_stop_search_returns_ok(app_client):
    """POST /search/stop -> 200 {"ok": true}."""
    reg = await _register_and_get_tokens(app_client, email="stop@example.com")

    response = await app_client.post(
        SEARCH_STOP_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- POST /search/stop without JWT → 401 ---


async def test_stop_search_without_jwt_returns_401(app_client):
    """POST /search/stop without Authorization header -> 401."""
    response = await app_client.post(SEARCH_STOP_URL)
    assert response.status_code == 401


# --- Test Strategy 5: GET /search/status without paired device ---


async def test_status_without_device_returns_offline(app_client):
    """GET /search/status with no paired device -> is_online=False, last_ping_at=None."""
    reg = await _register_and_get_tokens(app_client, email="nodev@example.com")

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["is_active"] is False
    assert data["is_online"] is False
    assert data["last_ping_at"] is None


# --- Test Strategy 6: GET /search/status with device (last_ping 10s ago) → is_online=True ---


async def test_status_with_recent_ping_is_online(app_client, db_session):
    """GET /search/status with device pinged 10s ago -> is_online=True."""
    reg = await _register_and_get_tokens(app_client, email="online@example.com")
    await _pair_device(app_client, reg["access_token"])

    # Set last_ping_at to 10 seconds ago directly in DB
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    device.last_ping_at = datetime.now(UTC) - timedelta(seconds=10)
    await db_session.commit()

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["is_online"] is True
    assert data["last_ping_at"] is not None


# --- Test Strategy 7: GET /search/status with device (last_ping 2 min ago) → is_online=False ---


async def test_status_with_stale_ping_is_offline(app_client, db_session):
    """GET /search/status with device pinged 2 min ago -> is_online=False."""
    reg = await _register_and_get_tokens(app_client, email="stale@example.com")
    await _pair_device(app_client, reg["access_token"])

    # Set last_ping_at to 2 minutes ago
    user_id = UUID(reg["user_id"])
    result = await db_session.execute(select(PairedDevice).where(PairedDevice.user_id == user_id))
    device = result.scalar_one()
    device.last_ping_at = datetime.now(UTC) - timedelta(seconds=120)
    await db_session.commit()

    response = await app_client.get(
        SEARCH_STATUS_URL,
        headers=_auth_header(reg["access_token"]),
    )

    assert response.status_code == 200
    data = response.json()
    assert data["is_online"] is False
    assert data["last_ping_at"] is not None


# --- Verify start actually sets is_active to true ---


async def test_start_then_status_shows_active(app_client):
    """POST /search/start then GET /search/status -> is_active=True."""
    reg = await _register_and_get_tokens(app_client, email="active@example.com")
    await _pair_device(app_client, reg["access_token"])
    headers = _auth_header(reg["access_token"])

    await app_client.post(SEARCH_START_URL, headers=headers)

    response = await app_client.get(SEARCH_STATUS_URL, headers=headers)

    assert response.status_code == 200
    assert response.json()["is_active"] is True


# --- Verify stop after start sets is_active to false ---


async def test_start_then_stop_then_status_shows_inactive(app_client):
    """POST /search/start then /stop then GET /search/status -> is_active=False."""
    reg = await _register_and_get_tokens(app_client, email="inactive@example.com")
    await _pair_device(app_client, reg["access_token"])
    headers = _auth_header(reg["access_token"])

    await app_client.post(SEARCH_START_URL, headers=headers)
    await app_client.post(SEARCH_STOP_URL, headers=headers)

    response = await app_client.get(SEARCH_STATUS_URL, headers=headers)

    assert response.status_code == 200
    assert response.json()["is_active"] is False


# ===== POST /search/device-override (Task 7.5) =====


# --- 7.5 Test 1: without device headers → 422 (missing required headers) ---


async def test_device_override_without_headers_returns_422(app_client):
    """POST /search/device-override without X-Device-* headers -> 422."""
    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": True},
    )
    assert response.status_code == 422


# --- 7.5 Test 2: invalid device token → 401 ---


async def test_device_override_with_invalid_token_returns_401(app_client):
    """POST /search/device-override with invalid device credentials -> 401."""
    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": True},
        headers=_device_headers("bad-token", "bad-device-id"),
    )
    assert response.status_code == 401


# --- 7.5 Test 3: valid device, active=True → 200, {"ok": true} ---


async def test_device_override_active_true_returns_ok(app_client):
    """POST /search/device-override with valid device, active=True -> 200."""
    reg = await _register_and_get_tokens(app_client, email="devoverride1@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-001")

    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": True},
        headers=_device_headers(pairing["device_token"], "override-dev-001"),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- 7.5 Test 4: valid device, active=False → 200, {"ok": true} ---


async def test_device_override_active_false_returns_ok(app_client):
    """POST /search/device-override with valid device, active=False -> 200."""
    reg = await _register_and_get_tokens(app_client, email="devoverride2@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-002")

    response = await app_client.post(
        DEVICE_OVERRIDE_URL,
        json={"active": False},
        headers=_device_headers(pairing["device_token"], "override-dev-002"),
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}


# --- 7.5 Test 5: verify is_active actually updates in DB for device's user ---


async def test_device_override_updates_is_active_in_db(app_client):
    """POST /search/device-override changes is_active, verified via GET /search/status."""
    reg = await _register_and_get_tokens(app_client, email="devoverride3@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-003")
    dev_hdrs = _device_headers(pairing["device_token"], "override-dev-003")
    jwt_hdrs = _auth_header(reg["access_token"])

    # Set active=True via device-override
    await app_client.post(DEVICE_OVERRIDE_URL, json={"active": True}, headers=dev_hdrs)

    # Verify via GET /search/status (JWT auth, Main App perspective)
    status_resp = await app_client.get(SEARCH_STATUS_URL, headers=jwt_hdrs)
    assert status_resp.json()["is_active"] is True

    # Set active=False via device-override
    await app_client.post(DEVICE_OVERRIDE_URL, json={"active": False}, headers=dev_hdrs)

    # Verify again
    status_resp = await app_client.get(SEARCH_STATUS_URL, headers=jwt_hdrs)
    assert status_resp.json()["is_active"] is False


# --- 7.5 Test 6: idempotent behavior ---


async def test_device_override_idempotent(app_client):
    """POST /search/device-override called twice with same value -> both 200."""
    reg = await _register_and_get_tokens(app_client, email="devoverride4@example.com")
    pairing = await _pair_device(app_client, reg["access_token"], device_id="override-dev-004")
    dev_hdrs = _device_headers(pairing["device_token"], "override-dev-004")

    resp1 = await app_client.post(DEVICE_OVERRIDE_URL, json={"active": True}, headers=dev_hdrs)
    resp2 = await app_client.post(DEVICE_OVERRIDE_URL, json={"active": True}, headers=dev_hdrs)

    assert resp1.status_code == 200
    assert resp2.status_code == 200
