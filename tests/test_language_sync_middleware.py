"""Isolated tests for language_sync dependency."""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.middleware.language_sync import sync_language_dependency
from app.models.user import User


class TestLanguageSyncSkipsWhenNoAuth:
    """Language sync does nothing when no authenticated user."""

    @pytest.mark.asyncio
    async def test_no_user_id_skips_update(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "uk"
        mock_db = AsyncMock()

        gen = sync_language_dependency(mock_request, mock_db)
        await gen.__anext__()  # yield point

        with patch("app.middleware.language_sync.user_id_ctx") as mock_ctx:
            mock_ctx.get.return_value = ""
            with contextlib.suppress(StopAsyncIteration):
                await gen.__anext__()

        mock_db.execute.assert_not_called()


class TestLanguageSyncSkipsWhenNoHeader:
    """Language sync does nothing when X-Language header is absent."""

    @pytest.mark.asyncio
    async def test_no_header_skips_update(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = None
        mock_db = AsyncMock()

        user_id = str(uuid4())

        gen = sync_language_dependency(mock_request, mock_db)
        await gen.__anext__()

        with patch("app.middleware.language_sync.user_id_ctx") as mock_ctx:
            mock_ctx.get.return_value = user_id
            with contextlib.suppress(StopAsyncIteration):
                await gen.__anext__()

        mock_db.execute.assert_not_called()


class TestLanguageSyncUpdatesLanguage:
    """Language sync updates user language when it differs."""

    @pytest.mark.asyncio
    async def test_updates_language_when_different(self, db_session, fake_redis, app_client):
        # Create a test user
        user = User(email="lang@example.com", password_hash="hash1", language="en")
        db_session.add(user)
        await db_session.commit()

        user_id = str(user.id)
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "uk-UA"

        gen = sync_language_dependency(mock_request, db_session)
        await gen.__anext__()

        with patch("app.middleware.language_sync.user_id_ctx") as mock_ctx:
            mock_ctx.get.return_value = user_id
            with contextlib.suppress(StopAsyncIteration):
                await gen.__anext__()

        # Verify language was updated
        result = await db_session.execute(select(User.language).where(User.id == user.id))
        updated_lang = result.scalar_one()
        assert updated_lang == "uk"


class TestLanguageSyncSkipsWhenSame:
    """Language sync skips update when language matches."""

    @pytest.mark.asyncio
    async def test_skips_when_language_matches(self, db_session, fake_redis, app_client):
        user = User(email="same@example.com", password_hash="hash1", language="en")
        db_session.add(user)
        await db_session.commit()

        user_id = str(user.id)
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "en-US"

        gen = sync_language_dependency(mock_request, db_session)
        await gen.__anext__()

        with patch("app.middleware.language_sync.user_id_ctx") as mock_ctx:
            mock_ctx.get.return_value = user_id
            with contextlib.suppress(StopAsyncIteration):
                await gen.__anext__()

        # Language should remain "en" — no update needed
        result = await db_session.execute(select(User.language).where(User.id == user.id))
        assert result.scalar_one() == "en"


class TestLanguageSyncHandlesErrors:
    """Language sync handles exceptions gracefully."""

    @pytest.mark.asyncio
    async def test_exception_does_not_propagate(self):
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "fr"
        mock_db = AsyncMock()
        mock_db.execute = AsyncMock(side_effect=RuntimeError("DB error"))

        user_id = str(uuid4())

        gen = sync_language_dependency(mock_request, mock_db)
        await gen.__anext__()

        with patch("app.middleware.language_sync.user_id_ctx") as mock_ctx:
            mock_ctx.get.return_value = user_id
            # Should not raise — logs debug instead
            with contextlib.suppress(StopAsyncIteration):
                await gen.__anext__()


class TestLanguageSyncParsesHeader:
    """Language sync correctly parses language from X-Language header."""

    @pytest.mark.asyncio
    async def test_parses_language_prefix(self, db_session, fake_redis, app_client):
        user = User(email="parse@example.com", password_hash="hash1", language="en")
        db_session.add(user)
        await db_session.commit()

        user_id = str(user.id)
        mock_request = MagicMock()
        # "es-MX" should be parsed to "es"
        mock_request.headers.get.return_value = "es-MX"

        gen = sync_language_dependency(mock_request, db_session)
        await gen.__anext__()

        with patch("app.middleware.language_sync.user_id_ctx") as mock_ctx:
            mock_ctx.get.return_value = user_id
            with contextlib.suppress(StopAsyncIteration):
                await gen.__anext__()

        result = await db_session.execute(select(User.language).where(User.id == user.id))
        assert result.scalar_one() == "es"
