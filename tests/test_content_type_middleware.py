"""Isolated tests for ContentTypeMiddleware."""

import pytest


class TestContentTypeMiddlewareRejectsInvalidContentType:
    """Mutating requests without application/json Content-Type are rejected."""

    @pytest.mark.asyncio
    async def test_post_without_json_content_type_returns_415(self, app_client):
        resp = await app_client.post(
            "/api/v1/auth/register",
            content="not json",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 415
        assert "application/json" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_put_without_json_content_type_returns_415(self, app_client):
        resp = await app_client.put(
            "/api/v1/auth/register",
            content="not json",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 415

    @pytest.mark.asyncio
    async def test_patch_without_json_content_type_returns_415(self, app_client):
        resp = await app_client.patch(
            "/api/v1/auth/register",
            content="not json",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 415

    @pytest.mark.asyncio
    async def test_missing_content_type_header_returns_415(self, app_client):
        resp = await app_client.post(
            "/api/v1/auth/register",
            content="{}",
            headers={"Content-Type": ""},
        )
        assert resp.status_code == 415


class TestContentTypeMiddlewareAllowsValidRequests:
    """Valid application/json requests pass through the middleware."""

    @pytest.mark.asyncio
    async def test_post_with_json_content_type_passes(self, app_client):
        resp = await app_client.post(
            "/api/v1/auth/register",
            json={"email": "ct@example.com", "password": "securePass1"},
        )
        # Should not be 415 — may be 201 (success) or 422 (validation), but not blocked
        assert resp.status_code != 415

    @pytest.mark.asyncio
    async def test_get_without_content_type_passes(self, app_client):
        resp = await app_client.get("/health")
        assert resp.status_code == 200


class TestContentTypeMiddlewareSkipsAdminPaths:
    """Admin paths are excluded from Content-Type validation."""

    @pytest.mark.asyncio
    async def test_admin_post_with_form_data_passes(self, app_client):
        resp = await app_client.post(
            "/admin/login",
            data={"username": "test", "password": "test"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        # Should not be 415 — admin paths skip the middleware
        assert resp.status_code != 415
