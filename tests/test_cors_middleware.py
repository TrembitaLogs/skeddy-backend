import pytest

from app.config import settings


def _valid_origin() -> str:
    cors_origins = [o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()]
    return cors_origins[0] if cors_origins else f"http://{settings.HOST}:{settings.PORT}"


@pytest.mark.asyncio
async def test_cors_preflight_returns_explicit_methods(app_client):
    """OPTIONS preflight must list explicit methods, not wildcard."""
    origin = _valid_origin()
    response = await app_client.options(
        "/health",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "POST",
        },
    )
    methods = response.headers.get("access-control-allow-methods", "")
    assert "*" not in methods
    for m in ("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
        assert m in methods


@pytest.mark.asyncio
async def test_cors_preflight_returns_explicit_headers(app_client):
    """OPTIONS preflight must list explicit headers, not wildcard."""
    origin = _valid_origin()
    response = await app_client.options(
        "/health",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Authorization",
        },
    )
    allowed = response.headers.get("access-control-allow-headers", "")
    assert "*" not in allowed
    assert "Authorization" in allowed or "authorization" in allowed


@pytest.mark.asyncio
async def test_cors_allows_credentials(app_client):
    """CORS must still allow credentials."""
    origin = _valid_origin()
    response = await app_client.options(
        "/health",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.headers.get("access-control-allow-credentials") == "true"


@pytest.mark.asyncio
async def test_cors_rejects_unknown_origin(app_client):
    """A request from an unknown origin should not get CORS headers."""
    response = await app_client.options(
        "/health",
        headers={
            "Origin": "https://evil.example.com",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert "access-control-allow-origin" not in response.headers
