"""Tests for Alembic billing migration (d7f2a8b31c04).

Test strategy:
1. alembic upgrade head -> migration successful
2. alembic downgrade -1 -> rollback successful
3. alembic upgrade head again -> idempotent
4. Verify all indexes created (pg_indexes query), including idx_purchase_orders_user
5. Verify CHECK constraints work at DB level

Tests are sync and ordered — each depends on the DB state from previous tests.
"""

import asyncio
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings

BACKEND_DIR = str(Path(__file__).resolve().parents[2])

# All named indexes created by the billing migration
EXPECTED_BILLING_INDEXES = {
    "idx_credit_balances_low",
    "idx_credit_transactions_user_created",
    "idx_credit_transactions_reference",
    "idx_purchase_orders_user",
    "idx_purchase_orders_consumed",
    "idx_rides_verification",
    "idx_rides_ride_hash",
}

BILLING_TABLES = {"credit_balances", "credit_transactions", "purchase_orders"}

RIDE_BILLING_COLUMNS = {
    "ride_hash",
    "verification_status",
    "verification_deadline",
    "verified_at",
    "disappeared_at",
    "last_reported_present",
    "last_verification_requested_at",
    "credits_charged",
    "credits_refunded",
}


def _run_alembic(*args: str) -> subprocess.CompletedProcess:
    """Run an alembic command via subprocess in the backend directory."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"alembic {' '.join(args)} failed (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
    return result


async def _fetch_rows(query: str, params: dict | None = None) -> list:
    """Execute a SELECT query and return all rows."""
    engine = create_async_engine(settings.DATABASE_URL)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            return result.fetchall()
    finally:
        await engine.dispose()


async def _execute_ddl(query: str, params: dict | None = None) -> None:
    """Execute a DDL/DML statement that does not return rows."""
    engine = create_async_engine(settings.DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(query), params or {})
    finally:
        await engine.dispose()


async def _exec_sql_expect_error(query: str, params: dict):
    """Execute SQL expecting IntegrityError; raise AssertionError if it succeeds."""
    engine = create_async_engine(settings.DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(query), params)
        raise AssertionError("Expected IntegrityError was not raised")
    except IntegrityError:
        pass  # expected
    finally:
        await engine.dispose()


def _query(query: str, params: dict | None = None) -> list:
    """Sync wrapper: execute SELECT and return rows."""
    return asyncio.run(_fetch_rows(query, params))


def _ddl(query: str, params: dict | None = None) -> None:
    """Sync wrapper: execute DDL/DML statement."""
    asyncio.run(_execute_ddl(query, params))


def _get_table_names() -> set[str]:
    rows = _query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    return {row[0] for row in rows}


def _get_index_names() -> set[str]:
    rows = _query("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
    return {row[0] for row in rows}


def _get_column_names(table: str) -> set[str]:
    rows = _query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = :table",
        {"table": table},
    )
    return {row[0] for row in rows}


# ---- Module setup: drop all tables for a clean slate ----


@pytest.fixture(scope="module", autouse=True)
def clean_db():
    """Drop all tables before migration tests; restore clean schema after."""
    _ddl("DROP SCHEMA public CASCADE")
    _ddl("CREATE SCHEMA public")
    yield
    _ddl("DROP SCHEMA public CASCADE")
    _ddl("CREATE SCHEMA public")


# ---- Test 1: alembic upgrade head ----


def test_upgrade_head():
    """Running alembic upgrade head creates all billing tables and ride columns."""
    _run_alembic("upgrade", "head")

    tables = _get_table_names()
    for table in BILLING_TABLES:
        assert table in tables, f"Table '{table}' not found after upgrade"

    ride_columns = _get_column_names("rides")
    for col in RIDE_BILLING_COLUMNS:
        assert col in ride_columns, f"Column 'rides.{col}' not found after upgrade"


# ---- Test 4: verify all indexes created ----


def test_all_indexes_exist():
    """All billing indexes exist after upgrade, including partial indexes."""
    indexes = _get_index_names()
    for idx in EXPECTED_BILLING_INDEXES:
        assert idx in indexes, f"Index '{idx}' not found"


# ---- Test 5: verify CHECK constraints at DB level ----


def test_check_constraint_negative_balance():
    """CHECK constraint rejects negative credit balance at DB level."""
    user_id = str(uuid.uuid4())
    _ddl(
        "INSERT INTO users (id, email, password_hash, email_verified) "
        "VALUES (:id, :email, :hash, false)",
        {"id": user_id, "email": "ck_bal@test.com", "hash": "hashed"},
    )
    try:
        asyncio.run(
            _exec_sql_expect_error(
                "INSERT INTO credit_balances (id, user_id, balance) VALUES (:id, :uid, -1)",
                {"id": str(uuid.uuid4()), "uid": user_id},
            )
        )
    finally:
        _ddl("DELETE FROM users WHERE id = :id", {"id": user_id})


def test_check_constraint_zero_credits_amount():
    """CHECK constraint rejects credits_amount <= 0 on purchase_orders."""
    user_id = str(uuid.uuid4())
    _ddl(
        "INSERT INTO users (id, email, password_hash, email_verified) "
        "VALUES (:id, :email, :hash, false)",
        {"id": user_id, "email": "ck_po@test.com", "hash": "hashed"},
    )
    try:
        asyncio.run(
            _exec_sql_expect_error(
                "INSERT INTO purchase_orders "
                "(id, user_id, product_id, purchase_token, credits_amount, status) "
                "VALUES (:id, :uid, 'credits_10', 'tok_ck', 0, 'PENDING')",
                {"id": str(uuid.uuid4()), "uid": user_id},
            )
        )
    finally:
        _ddl("DELETE FROM users WHERE id = :id", {"id": user_id})


def test_check_constraint_negative_credits_charged():
    """CHECK constraint rejects negative credits_charged on rides."""
    user_id = str(uuid.uuid4())
    _ddl(
        "INSERT INTO users (id, email, password_hash, email_verified) "
        "VALUES (:id, :email, :hash, false)",
        {"id": user_id, "email": "ck_ride@test.com", "hash": "hashed"},
    )
    try:
        asyncio.run(
            _exec_sql_expect_error(
                "INSERT INTO rides "
                "(id, user_id, idempotency_key, event_type, ride_data, "
                "ride_hash, credits_charged) "
                "VALUES (:id, :uid, :key, 'ACCEPTED', "
                "'{}'::jsonb, :hash, -1)",
                {
                    "id": str(uuid.uuid4()),
                    "uid": user_id,
                    "key": str(uuid.uuid4()),
                    "hash": "a" * 64,
                },
            )
        )
    finally:
        _ddl("DELETE FROM users WHERE id = :id", {"id": user_id})


# ---- Test 2: alembic downgrade -1 ----


def test_downgrade_removes_billing():
    """Running alembic downgrade to pre-billing revision removes billing tables and ride columns."""
    # Downgrade to the revision before billing models (d7f2a8b31c04).
    # Using explicit revision instead of -1 because the seed migration
    # (e5b3f1a82d09) sits between HEAD and the billing models migration.
    _run_alembic("downgrade", "c4a8e2f19b03")

    tables = _get_table_names()
    for table in BILLING_TABLES:
        assert table not in tables, f"Table '{table}' still exists after downgrade"

    assert "rides" in tables, "Table 'rides' should still exist"
    ride_columns = _get_column_names("rides")
    for col in RIDE_BILLING_COLUMNS:
        assert col not in ride_columns, f"Column 'rides.{col}' still exists after downgrade"

    indexes = _get_index_names()
    for idx in EXPECTED_BILLING_INDEXES:
        assert idx not in indexes, f"Index '{idx}' still exists after downgrade"


# ---- Test 3: alembic upgrade head again ----


def test_upgrade_after_downgrade():
    """Re-running alembic upgrade head after downgrade restores everything."""
    _run_alembic("upgrade", "head")

    tables = _get_table_names()
    for table in BILLING_TABLES:
        assert table in tables, f"Table '{table}' not found after re-upgrade"

    ride_columns = _get_column_names("rides")
    for col in RIDE_BILLING_COLUMNS:
        assert col in ride_columns, f"Column 'rides.{col}' not found after re-upgrade"

    indexes = _get_index_names()
    for idx in EXPECTED_BILLING_INDEXES:
        assert idx in indexes, f"Index '{idx}' not found after re-upgrade"
