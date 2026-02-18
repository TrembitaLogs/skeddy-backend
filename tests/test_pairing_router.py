from unittest.mock import AsyncMock
from uuid import UUID

from redis.exceptions import RedisError
from sqlalchemy import select

from app.main import app
from app.models.paired_device import PairedDevice
from app.models.search_status import SearchStatus
from app.redis import get_redis

GENERATE_URL = "/api/v1/pairing/generate"
CONFIRM_URL = "/api/v1/pairing/confirm"
STATUS_URL = "/api/v1/pairing/status"
UNPAIR_URL = "/api/v1/pairing"
REGISTER_URL = "/api/v1/auth/register"

_TEST_PASSWORD = "securePass1"


# --- Helpers ---


async def _register_and_get_token(client, email="pairing@example.com"):
    """Register a user and return (access_token, user_id)."""
    resp = await client.post(
        REGISTER_URL,
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 201
    data = resp.json()
    return data["access_token"], data["user_id"]


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


# --- Test 1: POST /pairing/generate without JWT → 401 ---


async def test_generate_without_jwt_returns_401(app_client):
    """POST /pairing/generate without Authorization header → 401/403."""
    response = await app_client.post(GENERATE_URL)
    assert response.status_code in (401, 403)


# --- Test 2: POST /pairing/generate with valid JWT → 201, 6-digit code ---


async def test_generate_with_valid_jwt_returns_201_with_code(app_client):
    """POST /pairing/generate with valid JWT → 201 with 6-digit code and expires_at."""
    token, _ = await _register_and_get_token(app_client)

    response = await app_client.post(GENERATE_URL, headers=_auth(token))

    assert response.status_code == 201
    data = response.json()
    assert "code" in data
    assert len(data["code"]) == 6
    assert data["code"].isdigit()
    assert 100000 <= int(data["code"]) <= 999999
    assert "expires_at" in data


# --- Test 3: POST /pairing/confirm with valid code → 200, device_token ---


async def test_confirm_with_valid_code_returns_200_with_device_token(app_client):
    """Full flow: generate code → confirm → 200 with device_token and user_id."""
    token, user_id = await _register_and_get_token(app_client)

    # Generate code
    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    assert gen.status_code == 201
    code = gen.json()["code"]

    # Confirm
    resp = await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "android-dev-001",
            "device_model": "Samsung SM-A156U",
            "timezone": "America/New_York",
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "device_token" in data
    UUID(data["device_token"])  # must be valid UUID
    assert data["user_id"] == user_id


# --- Test 3b: POST /pairing/confirm with device_model → stored in DB ---


async def test_confirm_stores_device_model_in_db(app_client, db_session):
    """Confirm pairing with device_model → device_model persisted in paired_devices."""
    token, user_id = await _register_and_get_token(app_client, "model@example.com")

    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]

    resp = await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "model-dev-001",
            "device_model": "Google Pixel 8 Pro",
            "timezone": "America/New_York",
        },
    )
    assert resp.status_code == 200

    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    device = result.scalar_one()
    assert device.device_model == "Google Pixel 8 Pro"


# --- Test 3c: POST /pairing/confirm without device_model → None in DB ---


async def test_confirm_without_device_model_stores_none(app_client, db_session):
    """Confirm pairing without device_model → device_model is None."""
    token, user_id = await _register_and_get_token(app_client, "nomodel@example.com")

    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]

    resp = await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "nomodel-dev-001",
            "timezone": "America/New_York",
        },
    )
    assert resp.status_code == 200

    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    device = result.scalar_one()
    assert device.device_model is None


# --- Test 4: POST /pairing/confirm with invalid code → 404 ---


async def test_confirm_with_invalid_code_returns_404(app_client):
    """POST /pairing/confirm with non-existent code → 404 PAIRING_CODE_EXPIRED."""
    response = await app_client.post(
        CONFIRM_URL,
        json={
            "code": "000000",
            "device_id": "dev-001",
            "timezone": "America/New_York",
        },
    )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "PAIRING_CODE_EXPIRED"


# --- Test 5: POST /pairing/confirm with invalid timezone → 422 ---


async def test_confirm_with_invalid_timezone_returns_422(app_client):
    """POST /pairing/confirm with invalid IANA timezone → 422 INVALID_TIMEZONE."""
    token, _ = await _register_and_get_token(app_client)

    # Generate a valid code first
    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]

    # Confirm with invalid timezone
    response = await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "dev-001",
            "timezone": "Not/A/Timezone",
        },
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_TIMEZONE"


# --- Test 6: POST /pairing/confirm with already-used code → 409 ---


async def test_confirm_with_used_code_returns_409(app_client):
    """POST /pairing/confirm with code that was already consumed → 409 PAIRING_CODE_USED."""
    token, _ = await _register_and_get_token(app_client)

    # Generate and consume code
    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]

    resp1 = await app_client.post(
        CONFIRM_URL,
        json={"code": code, "device_id": "dev-001", "timezone": "America/New_York"},
    )
    assert resp1.status_code == 200

    # Attempt to use the same code again
    resp2 = await app_client.post(
        CONFIRM_URL,
        json={"code": code, "device_id": "dev-002", "timezone": "America/New_York"},
    )

    assert resp2.status_code == 409
    assert resp2.json()["error"]["code"] == "PAIRING_CODE_USED"


# --- Test 7: Repeated POST /pairing/generate → new code, old code invalidated ---


async def test_repeated_generate_invalidates_old_code(app_client):
    """Calling generate twice invalidates the previous code (PRD requirement)."""
    token, _ = await _register_and_get_token(app_client)

    gen1 = await app_client.post(GENERATE_URL, headers=_auth(token))
    gen2 = await app_client.post(GENERATE_URL, headers=_auth(token))

    assert gen1.status_code == 201
    assert gen2.status_code == 201

    code1 = gen1.json()["code"]
    code2 = gen2.json()["code"]

    # Both are valid format
    assert len(code1) == 6 and code1.isdigit()
    assert len(code2) == 6 and code2.isdigit()

    # Old code must be invalidated — confirm with code1 should fail
    # (unless code1 == code2 by random chance, in which case code2
    # overwrote the same key and code1 still works — skip that edge case)
    if code1 != code2:
        resp = await app_client.post(
            CONFIRM_URL,
            json={
                "code": code1,
                "device_id": "dev-old",
                "timezone": "America/New_York",
            },
        )
        assert resp.status_code == 404

    # New code must be valid
    resp = await app_client.post(
        CONFIRM_URL,
        json={
            "code": code2,
            "device_id": "dev-new",
            "timezone": "America/New_York",
        },
    )
    assert resp.status_code == 200


# --- Test 8: Repeated confirm with new device_id → old device deleted ---


async def test_repeated_confirm_replaces_old_device(app_client, db_session):
    """Re-pairing the same user with a new device removes the old PairedDevice."""
    token, user_id = await _register_and_get_token(app_client)

    # First pairing
    gen1 = await app_client.post(GENERATE_URL, headers=_auth(token))
    code1 = gen1.json()["code"]
    resp1 = await app_client.post(
        CONFIRM_URL,
        json={"code": code1, "device_id": "old-device", "timezone": "US/Eastern"},
    )
    assert resp1.status_code == 200

    # Second pairing with new device_id
    gen2 = await app_client.post(GENERATE_URL, headers=_auth(token))
    code2 = gen2.json()["code"]
    resp2 = await app_client.post(
        CONFIRM_URL,
        json={"code": code2, "device_id": "new-device", "timezone": "US/Pacific"},
    )
    assert resp2.status_code == 200

    # Only the new device should exist for this user
    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    devices = result.scalars().all()
    assert len(devices) == 1
    assert devices[0].device_id == "new-device"
    assert devices[0].timezone == "US/Pacific"


# --- Bonus: Redis unavailability → 503 for both endpoints ---


async def test_generate_redis_unavailable_returns_503(app_client):
    """POST /pairing/generate when Redis is down → 503 SERVICE_UNAVAILABLE."""
    token, _ = await _register_and_get_token(app_client)

    # Override Redis with broken mock
    broken = AsyncMock()
    broken.get = AsyncMock(side_effect=RedisError("Connection refused"))
    broken.setex = AsyncMock(side_effect=RedisError("Connection refused"))
    app.dependency_overrides[get_redis] = lambda: broken

    response = await app_client.post(GENERATE_URL, headers=_auth(token))

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


async def test_confirm_redis_unavailable_returns_503(app_client):
    """POST /pairing/confirm when Redis is down → 503 SERVICE_UNAVAILABLE."""
    # Override Redis with broken mock
    broken = AsyncMock()
    broken.get = AsyncMock(side_effect=RedisError("Connection refused"))
    app.dependency_overrides[get_redis] = lambda: broken

    response = await app_client.post(
        CONFIRM_URL,
        json={"code": "123456", "device_id": "dev-001", "timezone": "America/New_York"},
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "SERVICE_UNAVAILABLE"


# --- GET /pairing/status ---


async def test_status_without_jwt_returns_401(app_client):
    """GET /pairing/status without Authorization header → 401/403."""
    response = await app_client.get(STATUS_URL)
    assert response.status_code in (401, 403)


async def test_status_not_paired_returns_false(app_client):
    """GET /pairing/status when user has no paired device → paired=false."""
    token, _ = await _register_and_get_token(app_client, "status-none@example.com")

    response = await app_client.get(STATUS_URL, headers=_auth(token))

    assert response.status_code == 200
    data = response.json()
    assert data["paired"] is False
    assert data["device_id"] is None
    assert data["device_model"] is None


async def test_status_paired_returns_device_info(app_client):
    """GET /pairing/status after pairing → paired=true with device_id and device_model."""
    token, _ = await _register_and_get_token(app_client, "status-paired@example.com")

    # Generate and confirm pairing
    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]
    await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "status-dev-001",
            "device_model": "Google Pixel 7",
            "timezone": "America/New_York",
        },
    )

    response = await app_client.get(STATUS_URL, headers=_auth(token))

    assert response.status_code == 200
    data = response.json()
    assert data["paired"] is True
    assert data["device_id"] == "status-dev-001"
    assert data["device_model"] == "Google Pixel 7"


async def test_status_paired_without_device_model(app_client):
    """GET /pairing/status after pairing without device_model → device_model is null."""
    token, _ = await _register_and_get_token(app_client, "status-nomodel@example.com")

    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]
    await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "status-dev-002",
            "timezone": "America/New_York",
        },
    )

    response = await app_client.get(STATUS_URL, headers=_auth(token))

    assert response.status_code == 200
    data = response.json()
    assert data["paired"] is True
    assert data["device_id"] == "status-dev-002"
    assert data["device_model"] is None


# --- DELETE /pairing ---


async def test_unpair_without_jwt_returns_401(app_client):
    """DELETE /pairing without Authorization header → 401/403."""
    response = await app_client.delete(UNPAIR_URL)
    assert response.status_code in (401, 403)


async def test_unpair_when_not_paired_returns_ok(app_client):
    """DELETE /pairing when no device paired → 200 ok (idempotent)."""
    token, _ = await _register_and_get_token(app_client, "unpair-none@example.com")

    response = await app_client.delete(UNPAIR_URL, headers=_auth(token))

    assert response.status_code == 200
    assert response.json()["ok"] is True


async def test_unpair_removes_device_and_deactivates_search(app_client, db_session):
    """DELETE /pairing removes PairedDevice and sets SearchStatus.is_active=false."""
    token, user_id = await _register_and_get_token(app_client, "unpair-full@example.com")

    # Pair a device
    gen = await app_client.post(GENERATE_URL, headers=_auth(token))
    code = gen.json()["code"]
    await app_client.post(
        CONFIRM_URL,
        json={
            "code": code,
            "device_id": "unpair-dev-001",
            "timezone": "America/New_York",
        },
    )

    # Start search so is_active=true
    await app_client.post("/api/v1/search/start", headers=_auth(token))

    # Unpair
    response = await app_client.delete(UNPAIR_URL, headers=_auth(token))
    assert response.status_code == 200
    assert response.json()["ok"] is True

    # Verify device deleted
    result = await db_session.execute(
        select(PairedDevice).where(PairedDevice.user_id == UUID(user_id))
    )
    assert result.scalar_one_or_none() is None

    # Verify search deactivated
    result = await db_session.execute(
        select(SearchStatus).where(SearchStatus.user_id == UUID(user_id))
    )
    status = result.scalar_one_or_none()
    assert status is not None
    assert status.is_active is False

    # Verify pairing status reflects unpaired
    status_resp = await app_client.get(STATUS_URL, headers=_auth(token))
    assert status_resp.json()["paired"] is False
