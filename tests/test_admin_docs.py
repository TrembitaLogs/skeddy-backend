"""Tests for protected API docs routes behind admin auth."""


class TestPublicDocsDisabled:
    """Verify that default FastAPI docs endpoints are disabled."""

    async def test_docs_returns_404(self, app_client):
        resp = await app_client.get("/docs")
        assert resp.status_code == 404

    async def test_redoc_returns_404(self, app_client):
        resp = await app_client.get("/redoc")
        assert resp.status_code == 404

    async def test_openapi_json_returns_404(self, app_client):
        resp = await app_client.get("/openapi.json")
        assert resp.status_code == 404


class TestAdminDocsUnauthenticated:
    """Verify that /admin/api-docs and /admin/api-openapi redirect without auth."""

    async def test_admin_docs_redirects_to_login(self, app_client):
        resp = await app_client.get("/admin/api-docs", follow_redirects=False)
        assert resp.status_code == 302
        assert "/admin/login" in resp.headers["location"]

    async def test_admin_openapi_redirects_to_login(self, app_client):
        resp = await app_client.get("/admin/api-openapi", follow_redirects=False)
        assert resp.status_code == 302
        assert "/admin/login" in resp.headers["location"]


class TestAdminDocsAuthenticated:
    """Verify that /admin/api-docs and /admin/api-openapi work after login."""

    async def test_admin_docs_returns_swagger_html(self, admin_client):
        resp = await admin_client.client.get("/admin/api-docs")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "swagger-ui" in resp.text

    async def test_admin_openapi_returns_valid_json(self, admin_client):
        resp = await admin_client.client.get("/admin/api-openapi")
        assert resp.status_code == 200
        assert "application/json" in resp.headers["content-type"]
        data = resp.json()
        assert "openapi" in data
        assert "paths" in data
