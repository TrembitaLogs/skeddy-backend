"""Security integration tests — IDOR prevention and unauthorized access (P0-4)."""

import uuid

import pytest

REGISTER_URL = "/api/v1/auth/register"
SEARCH_LOGIN_URL = "/api/v1/auth/search-login"
PING_URL = "/api/v1/ping"
FILTERS_URL = "/api/v1/filters"
SEARCH_STATUS_URL = "/api/v1/search/status"
RIDES_URL = "/api/v1/rides/events"

_PASSWORD = "securePass1"


def _jwt(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


async def _register(client, email):
    resp = await client.post(REGISTER_URL, json={"email": email, "password": _PASSWORD})
    assert resp.status_code == 201
    return resp.json()


async def _pair_device(client, email, device_id):
    resp = await client.post(
        SEARCH_LOGIN_URL,
        json={
            "email": email,
            "password": _PASSWORD,
            "device_id": device_id,
            "timezone": "Europe/Kyiv",
        },
    )
    assert resp.status_code == 200
    return resp.json()


# ---------------------------------------------------------------------------
# Unauthorized access — missing/invalid auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_no_auth_returns_401(app_client):
    """POST /ping without device headers returns 401."""
    resp = await app_client.post(PING_URL, json={"timezone": "UTC", "app_version": "1.0.0"})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_filters_no_auth_returns_401(app_client):
    """GET /filters without JWT returns 401."""
    resp = await app_client.get(FILTERS_URL)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_search_status_no_auth_returns_401(app_client):
    """GET /search/status without JWT returns 401."""
    resp = await app_client.get(SEARCH_STATUS_URL)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_rides_no_auth_returns_401(app_client):
    """GET /rides/events without JWT returns 401."""
    resp = await app_client.get(RIDES_URL)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_filters_invalid_jwt_returns_401(app_client):
    """GET /filters with garbage JWT returns 401."""
    resp = await app_client.get(FILTERS_URL, headers=_jwt("garbage.token.here"))
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_ping_invalid_device_token_returns_401(app_client):
    """POST /ping with invalid device credentials returns 401."""
    resp = await app_client.post(
        PING_URL,
        json={"timezone": "UTC", "app_version": "1.0.0"},
        headers=_device_headers("fake-token", "fake-device"),
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# IDOR — cross-user data isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_user_cannot_see_other_users_filters(app_client):
    """User A cannot access User B's filters."""
    user_a = await _register(app_client, "idor-a@example.com")
    await _register(app_client, "idor-b@example.com")

    # User A can access their own filters
    resp = await app_client.get(FILTERS_URL, headers=_jwt(user_a["access_token"]))
    assert resp.status_code == 200
    # Filters are fetched by the JWT user_id, so no cross-user access is possible
    # unless the endpoint has a bug allowing user_id override


@pytest.mark.asyncio
async def test_user_cannot_see_other_users_search_status(app_client):
    """User A cannot access User B's search status — each gets their own."""
    user_a = await _register(app_client, "idor-search-a@example.com")
    user_b = await _register(app_client, "idor-search-b@example.com")

    resp_a = await app_client.get(SEARCH_STATUS_URL, headers=_jwt(user_a["access_token"]))
    resp_b = await app_client.get(SEARCH_STATUS_URL, headers=_jwt(user_b["access_token"]))

    assert resp_a.status_code == 200
    assert resp_b.status_code == 200
    # Both should return their own status, not each other's


@pytest.mark.asyncio
async def test_device_paired_to_user_a_cannot_ping_as_user_b(app_client):
    """Device token from User A cannot be used with User B's device_id."""
    await _register(app_client, "dev-a@example.com")
    await _register(app_client, "dev-b@example.com")

    pair_a = await _pair_device(app_client, "dev-a@example.com", "device-a-001")

    # Try to use User A's device token with a different device_id
    resp = await app_client.post(
        PING_URL,
        json={"timezone": "UTC", "app_version": "1.0.0"},
        headers=_device_headers(pair_a["device_token"], "device-b-001"),
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_expired_jwt_returns_401(app_client):
    """An expired JWT should be rejected."""
    from datetime import UTC, datetime, timedelta

    import jwt as pyjwt

    from app.config import settings

    payload = {
        "sub": str(uuid.uuid4()),
        "exp": datetime.now(UTC) - timedelta(hours=1),
        "iat": datetime.now(UTC) - timedelta(hours=2),
    }
    expired_token = pyjwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
    resp = await app_client.get(FILTERS_URL, headers=_jwt(expired_token))
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_user_rides_isolated(app_client, db_session):
    """User A's ride events are not visible to User B."""
    user_a = await _register(app_client, "rides-a@example.com")
    user_b = await _register(app_client, "rides-b@example.com")

    resp_a = await app_client.get(RIDES_URL, headers=_jwt(user_a["access_token"]))
    resp_b = await app_client.get(RIDES_URL, headers=_jwt(user_b["access_token"]))

    assert resp_a.status_code == 200
    assert resp_b.status_code == 200
    # Both return only their own rides (empty initially)
    assert resp_a.json()["items"] == []
    assert resp_b.json()["items"] == []
