import time
from unittest.mock import AsyncMock, patch

import pytest

HEALTH_URL = "/health"
DETAIL_KEY = "test-admin-secret"
HEALTH_DETAIL_URL = f"/health?detail={DETAIL_KEY}"


@pytest.fixture(autouse=True)
def _admin_secret(monkeypatch):
    """Ensure ADMIN_SECRET_KEY is set for detail tests."""
    from app.config import settings

    monkeypatch.setattr(settings, "ADMIN_SECRET_KEY", DETAIL_KEY)


@pytest.mark.asyncio
async def test_health_returns_status_only_without_detail_key(app_client):
    """GET /health without detail key returns only status field."""
    response = await app_client.get(HEALTH_URL)
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert data["status"] in ("ok", "degraded")
    assert "postgres" not in data
    assert "redis" not in data


@pytest.mark.asyncio
async def test_health_returns_full_json_with_detail_key(app_client):
    """GET /health?detail=<key> returns status, postgres, redis fields."""
    response = await app_client.get(HEALTH_DETAIL_URL)
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "postgres" in data
    assert "redis" in data
    assert data["status"] in ("ok", "degraded")
    assert data["postgres"] in ("ok", "unavailable")
    assert data["redis"] in ("ok", "unavailable")


@pytest.mark.asyncio
async def test_health_rejects_wrong_detail_key(app_client):
    """GET /health?detail=wrong returns only status, no component details."""
    response = await app_client.get("/health?detail=wrong-key")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "postgres" not in data
    assert "redis" not in data


@pytest.mark.asyncio
async def test_health_not_available_on_api_v1(app_client):
    """Health endpoint is on /health, not /api/v1/health."""
    response = await app_client.get("/api/v1/health")
    assert response.status_code in (404, 405)


@pytest.mark.asyncio
async def test_health_postgres_ok_when_db_available(app_client):
    """GET /health returns postgres: 'ok' when PostgreSQL is reachable."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=None)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("app.main.AsyncSessionLocal", return_value=mock_session):
        response = await app_client.get(HEALTH_DETAIL_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["postgres"] == "ok"


@pytest.mark.asyncio
async def test_health_postgres_unavailable_when_db_down(app_client):
    """GET /health returns postgres: 'unavailable' and status: 'degraded' when PostgreSQL is down."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=ConnectionRefusedError("connection refused"))
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("app.main.AsyncSessionLocal", return_value=mock_session):
        response = await app_client.get(HEALTH_DETAIL_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["postgres"] == "unavailable"
    assert data["status"] == "degraded"


@pytest.mark.asyncio
async def test_health_redis_ok_when_available(app_client):
    """GET /health returns redis: 'ok' when Redis is reachable."""
    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(return_value=True)

    with patch("app.main.redis_client", mock_redis):
        response = await app_client.get(HEALTH_DETAIL_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["redis"] == "ok"


@pytest.mark.asyncio
async def test_health_redis_unavailable_when_down(app_client):
    """GET /health returns redis: 'unavailable' and status: 'degraded' when Redis is down."""
    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(side_effect=ConnectionError("connection refused"))

    with patch("app.main.redis_client", mock_redis):
        response = await app_client.get(HEALTH_DETAIL_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["redis"] == "unavailable"
    assert data["status"] == "degraded"


@pytest.mark.asyncio
async def test_health_status_ok_when_all_services_available(app_client):
    """GET /health returns status: 'ok' when both PostgreSQL and Redis are reachable."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=None)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(return_value=True)

    with (
        patch("app.main.AsyncSessionLocal", return_value=mock_session),
        patch("app.main.redis_client", mock_redis),
    ):
        response = await app_client.get(HEALTH_DETAIL_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["postgres"] == "ok"
    assert data["redis"] == "ok"


@pytest.mark.asyncio
async def test_health_both_unavailable_when_all_down(app_client):
    """GET /health returns both 'unavailable' when PostgreSQL and Redis are both down."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=ConnectionRefusedError("pg down"))
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(side_effect=ConnectionError("redis down"))

    with (
        patch("app.main.AsyncSessionLocal", return_value=mock_session),
        patch("app.main.redis_client", mock_redis),
    ):
        response = await app_client.get(HEALTH_DETAIL_URL)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "degraded"
    assert data["postgres"] == "unavailable"
    assert data["redis"] == "unavailable"


@pytest.mark.asyncio
async def test_health_response_time(app_client):
    """GET /health completes within 500ms."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=None)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(return_value=True)

    with (
        patch("app.main.AsyncSessionLocal", return_value=mock_session),
        patch("app.main.redis_client", mock_redis),
    ):
        start = time.monotonic()
        response = await app_client.get(HEALTH_URL)
        elapsed_ms = (time.monotonic() - start) * 1000

    assert response.status_code == 200
    assert elapsed_ms < 500, f"Health check took {elapsed_ms:.0f}ms, expected <500ms"
