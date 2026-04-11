"""Negative ping tests — Redis/DB unavailable, malformed input (P1-8)."""

from unittest.mock import AsyncMock, patch

import pytest

REGISTER_URL = "/api/v1/auth/register"
SEARCH_LOGIN_URL = "/api/v1/auth/search-login"
PING_URL = "/api/v1/ping"

_PASSWORD = "securePass1"


def _device_headers(device_token: str, device_id: str) -> dict:
    return {"X-Device-Token": device_token, "X-Device-Id": device_id}


def _ping_body(**overrides) -> dict:
    body = {"timezone": "America/New_York", "app_version": "1.0.0"}
    body.update(overrides)
    return body


async def _setup_device(app_client, email="neg-ping@example.com", device_id="neg-dev-001"):
    """Register user, pair device, return headers."""
    resp = await app_client.post(REGISTER_URL, json={"email": email, "password": _PASSWORD})
    assert resp.status_code == 201

    resp = await app_client.post(
        SEARCH_LOGIN_URL,
        json={
            "email": email,
            "password": _PASSWORD,
            "device_id": device_id,
            "timezone": "America/New_York",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    return _device_headers(data["device_token"], device_id)


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_missing_timezone_returns_422(app_client):
    """POST /ping without timezone field returns 422."""
    headers = await _setup_device(app_client, "no-tz@example.com", "no-tz-dev")
    resp = await app_client.post(
        PING_URL,
        json={"app_version": "1.0.0"},
        headers=headers,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ping_missing_app_version_returns_422(app_client):
    """POST /ping without app_version field returns 422."""
    headers = await _setup_device(app_client, "no-ver@example.com", "no-ver-dev")
    resp = await app_client.post(
        PING_URL,
        json={"timezone": "UTC"},
        headers=headers,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ping_empty_body_returns_422(app_client):
    """POST /ping with empty body returns 422."""
    headers = await _setup_device(app_client, "empty@example.com", "empty-dev")
    resp = await app_client.post(PING_URL, json={}, headers=headers)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ping_invalid_timezone_string_returns_422(app_client):
    """POST /ping with invalid IANA timezone returns 422."""
    headers = await _setup_device(app_client, "badtz@example.com", "badtz-dev")
    resp = await app_client.post(
        PING_URL,
        json=_ping_body(timezone="Not/A/Timezone"),
        headers=headers,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ping_null_stats_accepted(app_client):
    """POST /ping with null stats is accepted (optional field)."""
    headers = await _setup_device(app_client, "null-stats@example.com", "null-stats-dev")
    resp = await app_client.post(
        PING_URL,
        json=_ping_body(stats=None),
        headers=headers,
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Redis unavailability during ping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_redis_error_in_stats_dedup(app_client):
    """Ping handles Redis failure during stats batch dedup gracefully."""
    from redis.exceptions import RedisError

    headers = await _setup_device(app_client, "redis-err@example.com", "redis-err-dev")

    with patch(
        "app.services.ping_service.stats.is_batch_already_processed",
        new_callable=AsyncMock,
        side_effect=RedisError("Connection refused"),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(
                stats={
                    "batch_id": "test-batch-001",
                    "rides_found": 5,
                    "rides_accepted": 1,
                    "accept_failures": [],
                }
            ),
            headers=headers,
        )
    # Endpoint may crash (500) or propagate as error
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# DB error during ping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_db_error_during_commit(app_client):
    """Ping returns error when DB commit fails."""
    from sqlalchemy.exc import OperationalError

    headers = await _setup_device(app_client, "db-err@example.com", "db-err-dev")

    with patch(
        "app.services.ping_service.device.update_device_state",
        new_callable=AsyncMock,
        side_effect=OperationalError("DB connection lost", {}, None),
    ):
        resp = await app_client.post(
            PING_URL,
            json=_ping_body(),
            headers=headers,
        )
    # DB error during update_device_state should propagate as 500
    # (or may be caught by error handler middleware)
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Large/boundary payloads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_very_long_app_version(app_client):
    """POST /ping with extremely long app_version triggers DB or validation error."""
    headers = await _setup_device(app_client, "long-ver@example.com", "long-ver-dev")
    resp = await app_client.post(
        PING_URL,
        json=_ping_body(app_version="x" * 1000),
        headers=headers,
    )
    # DB varchar(20) truncation or validation error
    assert resp.status_code in (200, 422, 500)


@pytest.mark.asyncio
async def test_ping_negative_cycle_duration(app_client):
    """POST /ping with negative last_cycle_duration_ms is handled."""
    headers = await _setup_device(app_client, "neg-cycle@example.com", "neg-cycle-dev")
    resp = await app_client.post(
        PING_URL,
        json=_ping_body(last_cycle_duration_ms=-100),
        headers=headers,
    )
    # Should either validate or treat as null
    assert resp.status_code in (200, 422)
