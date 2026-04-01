import pytest


@pytest.mark.asyncio
async def test_csrf_allows_admin_post_without_origin(app_client):
    """POST to /admin/ without Origin or Referer is allowed (same-origin or non-browser)."""
    response = await app_client.post("/admin/login", data={"username": "a", "password": "b"})
    # No Origin/Referer → CSRF middleware allows the request through;
    # the response will be a redirect or auth error, not 403 CSRF.
    assert response.status_code != 403


@pytest.mark.asyncio
async def test_csrf_blocks_admin_post_with_wrong_origin(app_client):
    """POST to /admin/ from a foreign origin returns 403."""
    response = await app_client.post(
        "/admin/login",
        data={"username": "a", "password": "b"},
        headers={"Origin": "https://evil.example.com"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_csrf_allows_admin_get(app_client):
    """GET requests to admin are not subject to CSRF checks."""
    response = await app_client.get("/admin/login")
    # Should not be 403 — admin login page renders normally
    assert response.status_code != 403


@pytest.mark.asyncio
async def test_csrf_allows_api_v1_without_origin(app_client):
    """Mobile API endpoints (JWT-based) are not protected by CSRF."""
    response = await app_client.post("/api/v1/auth/login", json={"email": "x", "password": "y"})
    # Should not be 403 — mobile API is exempt from CSRF
    assert response.status_code != 403


@pytest.mark.asyncio
async def test_csrf_allows_admin_post_with_valid_referer(app_client):
    """POST to /admin/ with a valid Referer passes CSRF check."""
    from app.config import settings

    cors_origins = [o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()]
    valid_origin = cors_origins[0] if cors_origins else f"http://{settings.HOST}:{settings.PORT}"

    response = await app_client.post(
        "/admin/login",
        data={"username": "a", "password": "b"},
        headers={"Referer": f"{valid_origin}/admin/login"},
    )
    # Not 403 — CSRF passed; may be 400/401 due to bad credentials, that's fine
    assert response.status_code != 403
