"""Tests for Alembic billing config seed migration (e5b3f1a82d09).

Test strategy:
1. alembic upgrade head -> all 5 billing keys exist
2. alembic upgrade head again (on existing keys) -> idempotent (no duplicates)
3. alembic downgrade -1 -> removes only billing keys, not others
"""

import asyncio
import json
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import TEST_DATABASE_URL

BACKEND_DIR = str(Path(__file__).resolve().parents[2])

BILLING_KEYS = {
    "credit_products",
    "ride_credit_tiers",
    "registration_bonus_credits",
    "verification_deadline_minutes",
    "verification_check_interval_minutes",
}

# Keys seeded by earlier migrations that must survive downgrade
PRE_EXISTING_KEYS = {
    "min_search_app_version",
    "requests_per_day",
    "requests_per_hour",
}


def _async_test_url() -> str:
    """Return the async test URL for alembic subprocess (env.py uses asyncpg)."""
    return TEST_DATABASE_URL


def _run_alembic(*args: str) -> subprocess.CompletedProcess:
    """Run an alembic command via subprocess against the test database."""
    import os

    env = os.environ.copy()
    env["DATABASE_URL"] = _async_test_url()
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


async def _fetch_rows(query: str, params: dict | None = None) -> list:
    """Execute a SELECT query and return all rows."""
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            return result.fetchall()
    finally:
        await engine.dispose()


async def _execute_ddl(query: str, params: dict | None = None) -> None:
    """Execute a DDL/DML statement that does not return rows."""
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(query), params or {})
    finally:
        await engine.dispose()


def _query(query: str, params: dict | None = None) -> list:
    """Sync wrapper: execute SELECT and return rows."""
    return asyncio.run(_fetch_rows(query, params))


def _ddl(query: str, params: dict | None = None) -> None:
    """Sync wrapper: execute DDL/DML statement."""
    asyncio.run(_execute_ddl(query, params))


def _get_app_config_keys() -> dict[str, str]:
    """Return all app_configs rows as {key: value}."""
    rows = _query("SELECT key, value FROM app_configs")
    return {row[0]: row[1] for row in rows}


# ---- Module setup: clean slate ----


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


# ---- Test 1: alembic upgrade head seeds all billing keys ----


def test_upgrade_seeds_billing_keys():
    """Running alembic upgrade head creates all 5 billing AppConfig keys."""
    _run_alembic("upgrade", "head")

    configs = _get_app_config_keys()
    for key in BILLING_KEYS:
        assert key in configs, f"Billing key '{key}' not found after upgrade"

    # Verify values are parseable
    products = json.loads(configs["credit_products"])
    assert len(products) == 4
    assert products[0]["product_id"] == "credits_10"

    tiers = json.loads(configs["ride_credit_tiers"])
    assert len(tiers) == 3
    assert tiers[2]["max_price"] is None

    assert configs["registration_bonus_credits"] == "10"
    assert configs["verification_deadline_minutes"] == "30"
    assert configs["verification_check_interval_minutes"] == "60"


# ---- Test 2: idempotent re-upgrade ----


def test_upgrade_idempotent_no_duplicates():
    """Running alembic upgrade head again on existing keys does not create duplicates."""
    # Already at head from test_upgrade_seeds_billing_keys
    _run_alembic("upgrade", "head")

    configs = _get_app_config_keys()
    for key in BILLING_KEYS:
        assert key in configs

    # Count rows — each key must appear exactly once (primary key enforces this,
    # but ON CONFLICT DO NOTHING also ensures no error)
    rows = _query("SELECT COUNT(*) FROM app_configs WHERE key = 'credit_products'")
    assert rows[0][0] == 1


# ---- Test 3: downgrade removes billing keys, keeps others ----


def test_downgrade_removes_only_billing_keys():
    """Running alembic downgrade to pre-billing revision removes billing keys but keeps pre-existing ones."""
    # Ensure we are at HEAD first (no-op if already there from previous tests,
    # but necessary when this test is run in isolation).
    _run_alembic("upgrade", "head")
    # Downgrade to the revision just before e5b3f1a82d09 (billing seed),
    # not just -1 from HEAD, because newer migrations may sit after it.
    _run_alembic("downgrade", "d7f2a8b31c04")

    configs = _get_app_config_keys()

    # Billing keys should be gone
    for key in BILLING_KEYS:
        assert key not in configs, f"Billing key '{key}' still exists after downgrade"

    # Pre-existing keys should survive
    for key in PRE_EXISTING_KEYS:
        assert key in configs, f"Pre-existing key '{key}' was removed by downgrade"


# ---- Test 4: re-upgrade after downgrade ----


def test_upgrade_after_downgrade_restores_keys():
    """Re-running alembic upgrade head after downgrade restores billing keys."""
    _run_alembic("upgrade", "head")

    configs = _get_app_config_keys()
    for key in BILLING_KEYS:
        assert key in configs, f"Billing key '{key}' not found after re-upgrade"
