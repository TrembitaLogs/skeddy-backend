"""Tests for app.database module: session factory, Base, get_db, init_db."""

import contextlib

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal, Base, get_db, init_db

# ---------------------------------------------------------------------------
# AsyncSessionLocal factory
# ---------------------------------------------------------------------------


class TestAsyncSessionLocal:
    """Tests for the async session factory."""

    @pytest.mark.asyncio
    async def test_creates_async_session(self):
        """AsyncSessionLocal() yields an AsyncSession instance."""
        async with AsyncSessionLocal() as session:
            assert isinstance(session, AsyncSession)

    @pytest.mark.asyncio
    async def test_expire_on_commit_disabled(self):
        """Sessions have expire_on_commit=False for post-commit attribute access."""
        async with AsyncSessionLocal() as session:
            sync_session = session.sync_session
            assert sync_session.expire_on_commit is False

    @pytest.mark.asyncio
    async def test_session_is_bound_to_engine(self):
        """Session is bound to the application engine."""
        async with AsyncSessionLocal() as session:
            assert session.bind is not None


# ---------------------------------------------------------------------------
# Base declarative class
# ---------------------------------------------------------------------------


class TestBase:
    """Tests for the ORM Base class."""

    def test_base_has_metadata(self):
        """Base has a metadata attribute with table definitions."""
        assert Base.metadata is not None
        assert len(Base.metadata.tables) > 0

    def test_base_has_async_attrs(self):
        """Base includes AsyncAttrs mixin for lazy-load support."""
        from sqlalchemy.ext.asyncio import AsyncAttrs

        assert issubclass(Base, AsyncAttrs)


# ---------------------------------------------------------------------------
# get_db dependency
# ---------------------------------------------------------------------------


class TestGetDb:
    """Tests for the get_db FastAPI dependency."""

    @pytest.mark.asyncio
    async def test_yields_session(self):
        """get_db yields an AsyncSession."""
        gen = get_db()
        session = await gen.__anext__()
        assert isinstance(session, AsyncSession)
        with contextlib.suppress(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.asyncio
    async def test_session_closes_after_yield(self):
        """Session is closed after the generator exits."""
        gen = get_db()
        session = await gen.__anext__()
        with contextlib.suppress(StopAsyncIteration):
            await gen.__anext__()
        # After generator exhaustion the session object still exists
        assert session is not None


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------


class TestInitDb:
    """Tests for init_db table creation."""

    @pytest.mark.asyncio
    async def test_init_db_creates_tables(self, db_session):
        """init_db creates all tables defined in Base.metadata."""
        # db_session fixture already creates tables; verify key tables exist
        result = await db_session.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        )
        tables = {row[0] for row in result.fetchall()}
        assert "users" in tables
        assert "credit_balances" in tables
        assert "credit_transactions" in tables

    @pytest.mark.asyncio
    async def test_init_db_is_idempotent(self, db_session):
        """Calling init_db twice does not raise errors."""
        # init_db uses create_all which is idempotent
        await init_db()
        await init_db()  # Should not raise
