import pytest

HEALTH_URL = "/health"


@pytest.mark.asyncio
async def test_security_headers_present(app_client):
    """All security headers are set on every response."""
    response = await app_client.get(HEALTH_URL)

    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert "max-age=" in response.headers["Strict-Transport-Security"]
    assert "default-src" in response.headers["Content-Security-Policy"]
    assert response.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "camera=()" in response.headers["Permissions-Policy"]


@pytest.mark.asyncio
async def test_api_csp_is_strict(app_client):
    """API routes get a strict CSP with no inline scripts/styles."""
    csp = (await app_client.get(HEALTH_URL)).headers["Content-Security-Policy"]
    assert "default-src 'none'" in csp
    assert "'unsafe-inline'" not in csp


@pytest.mark.asyncio
async def test_csp_allows_cdn_for_admin(app_client):
    """CSP allows jsdelivr CDN scripts/styles required by SQLAdmin panel."""
    csp = (await app_client.get("/admin/")).headers["Content-Security-Policy"]
    assert "https://cdn.jsdelivr.net" in csp
    assert "'unsafe-inline'" in csp


@pytest.mark.asyncio
async def test_auth_endpoint_has_cache_control_no_store(app_client):
    """Auth endpoints set Cache-Control: no-store to prevent token caching."""
    resp = await app_client.post("/api/v1/auth/login", json={"email": "x", "password": "y"})
    assert resp.headers.get("Cache-Control") == "no-store"


@pytest.mark.asyncio
async def test_non_sensitive_endpoint_has_no_cache_control(app_client):
    """Non-sensitive endpoints do not set Cache-Control: no-store."""
    resp = await app_client.get(HEALTH_URL)
    assert "no-store" not in resp.headers.get("Cache-Control", "")
