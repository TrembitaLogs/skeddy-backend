"""Tests for Alembic WELCOME email template seed migration.

Strategy:
1. alembic upgrade head -> WELCOME row exists with non-empty subjects/bodies (en + es).
2. Re-run upgrade -> idempotent (no duplicate, ON CONFLICT DO NOTHING).
3. alembic downgrade -1 -> WELCOME row removed; other email templates remain.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import TEST_DATABASE_URL

BACKEND_DIR = str(Path(__file__).resolve().parents[2])
WELCOME_REVISION = "b1c2d3e4f5a6"
PRIOR_REVISION = "e9f74122e224"


def _run_alembic(*args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["DATABASE_URL"] = TEST_DATABASE_URL
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_DIR,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"alembic {' '.join(args)} failed (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
    return result


async def _execute_ddl(query: str) -> None:
    """Execute a DDL/DML statement that does not return rows."""
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(query))
    finally:
        await engine.dispose()


def _ddl(query: str) -> None:
    """Sync wrapper: execute DDL/DML statement."""
    asyncio.run(_execute_ddl(query))


# ---- Module setup: drop all tables for a clean slate ----


@pytest.fixture(scope="module", autouse=True)
def clean_db():
    """Drop all tables before migration tests; restore clean schema after."""
    _ddl("DROP SCHEMA public CASCADE")
    _ddl("CREATE SCHEMA public")
    yield
    _ddl("DROP SCHEMA public CASCADE")
    _ddl("CREATE SCHEMA public")
    # Restore tables so subsequent test modules find the schema intact
    from app.database import Base
    from tests.conftest import _test_engine

    async def _restore():
        async with _test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    asyncio.run(_restore())


async def _fetch_email_types(engine) -> set[str]:
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT email_type FROM email_templates"))
        return {row[0] for row in result.fetchall()}


async def _fetch_welcome_row(engine) -> dict | None:
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT email_type, subject_en, body_en, subject_es, body_es "
                "FROM email_templates WHERE email_type = 'WELCOME'"
            )
        )
        row = result.first()
        if row is None:
            return None
        return {
            "email_type": row[0],
            "subject_en": row[1],
            "body_en": row[2],
            "subject_es": row[3],
            "body_es": row[4],
        }


@pytest.mark.asyncio
async def test_welcome_migration_seeds_row():
    _run_alembic("upgrade", "head")
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        row = await _fetch_welcome_row(engine)
        assert row is not None, "WELCOME row should be seeded after upgrade"
        assert row["subject_en"]
        assert row["body_en"]
        assert row["subject_es"]
        assert row["body_es"]
        assert "{search_app_url}" in row["body_en"]
        assert "{bonus_amount}" in row["body_en"]
        assert "{search_app_url}" in row["body_es"]
        assert "{bonus_amount}" in row["body_es"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_welcome_migration_idempotent():
    _run_alembic("upgrade", "head")
    _run_alembic("upgrade", "head")  # second run must not fail or duplicate
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM email_templates WHERE email_type = 'WELCOME'")
            )
            count = result.scalar_one()
            assert count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_welcome_migration_downgrade_removes_only_welcome():
    _run_alembic("upgrade", "head")
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        before = await _fetch_email_types(engine)
        assert "WELCOME" in before
    finally:
        await engine.dispose()

    _run_alembic("downgrade", PRIOR_REVISION)

    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        after = await _fetch_email_types(engine)
        assert "WELCOME" not in after
        # Pre-existing templates must survive
        assert {"VERIFICATION", "EMAIL_CHANGE", "PASSWORD_RESET"}.issubset(after)
    finally:
        await engine.dispose()
        # Restore head for subsequent tests in the suite
        _run_alembic("upgrade", "head")
