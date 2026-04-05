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
