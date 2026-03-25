"""Tests for automatic user language update from X-Language header."""

from uuid import UUID

from sqlalchemy import select

from app.models.user import User


class TestLanguageUpdate:
    """Tests for language detection via X-Language header in get_current_user."""

    async def test_language_updated_from_header(self, authenticated_client, db_session):
        """Language is updated when X-Language header differs from stored value."""
        client = authenticated_client.client
        headers = {**authenticated_client.headers, "X-Language": "es-MX"}

        response = await client.get("/api/v1/auth/me", headers=headers)
        assert response.status_code == 200

        result = await db_session.execute(
            select(User.language).where(User.id == UUID(authenticated_client.user_id))
        )
        assert result.scalar_one() == "es"

    async def test_language_not_updated_when_same(self, authenticated_client, db_session):
        """No DB update when X-Language matches stored language."""
        client = authenticated_client.client
        headers = {**authenticated_client.headers, "X-Language": "en-US"}

        response = await client.get("/api/v1/auth/me", headers=headers)
        assert response.status_code == 200

        result = await db_session.execute(
            select(User.language).where(User.id == UUID(authenticated_client.user_id))
        )
        assert result.scalar_one() == "en"

    async def test_language_base_extracted(self, authenticated_client, db_session):
        """Full locale like 'es-419' is reduced to base 'es'."""
        client = authenticated_client.client
        headers = {**authenticated_client.headers, "X-Language": "es-419"}

        response = await client.get("/api/v1/auth/me", headers=headers)
        assert response.status_code == 200

        result = await db_session.execute(
            select(User.language).where(User.id == UUID(authenticated_client.user_id))
        )
        assert result.scalar_one() == "es"

    async def test_no_header_no_change(self, authenticated_client, db_session):
        """Language stays default when X-Language header is absent."""
        client = authenticated_client.client
        headers = {k: v for k, v in authenticated_client.headers.items() if k != "X-Language"}

        response = await client.get("/api/v1/auth/me", headers=headers)
        assert response.status_code == 200

        result = await db_session.execute(
            select(User.language).where(User.id == UUID(authenticated_client.user_id))
        )
        assert result.scalar_one() == "en"
